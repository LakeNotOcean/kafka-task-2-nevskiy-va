#!/usr/bin/env python3

import json
import os
import re
import sqlite3
import subprocess
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DATA = ROOT / "data"
SNAPSHOT_PATH = DATA / "sink-snapshot.db"
DLT_DUMP_PATH = DATA / "dlt-dump.txt"
DETAIL_LIMIT = 10


def run_command(*command):
    process = subprocess.run(command, capture_output=True, text=True, cwd=ROOT)
    if process.returncode:
        sys.exit(f"{' '.join(command)}\n{process.stderr[-1000:]}")
    return process.stdout


def github_login():
    value = os.environ.get("GITHUB_LOGIN", "")
    env_file = ROOT / ".env"
    if not value and env_file.exists():
        for line in env_file.read_text().splitlines():
            if line.startswith("GITHUB_LOGIN="):
                value = line.split("=", 1)[1].strip()
                break
    if not value:
        sys.exit("GITHUB_LOGIN не задан")
    return value

# нормализация до микросекунд
def parse_timestamp(timestamp):
    normalized = re.sub(r"\.(\d{6})\d+", r".\1", timestamp)
    return datetime.fromisoformat(normalized)


def dlt_message_count():
    # сумма end-offsets по партициям: сколько сообщений сейчас в DLT 
    # используется kafka-1
    output = run_command(
        "docker", "compose", "exec", "-T", "kafka-1",
        "/opt/kafka/bin/kafka-get-offsets.sh",
        "--bootstrap-server", "localhost:9092", "--topic", "events.dlt",
        "--time", "-1",
    )
    total = 0
    for line in output.splitlines():
        parts = line.split(":")
        if len(parts) == 3:
            total += int(parts[2])
    return total


def main():
    print(github_login(), datetime.now().isoformat(timespec="seconds"))

    if "--skip-refresh" not in sys.argv:
        DATA.mkdir(exist_ok=True)
        run_command("docker", "compose", "run", "--rm", "sink-snapshot")
        message_count = dlt_message_count()
        if message_count > 0:
            dlt_dump = run_command(
                "docker", "compose", "exec", "-T", "kafka-1",
                "/opt/kafka/bin/kafka-console-consumer.sh",
                "--bootstrap-server", "localhost:9092", "--topic", "events.dlt",
                "--from-beginning", "--max-messages", str(message_count),
                "--timeout-ms", "10000",
            )
        else:
            dlt_dump = ""
        DLT_DUMP_PATH.write_text(dlt_dump, encoding="utf-8")

    sink_db = sqlite3.connect(SNAPSHOT_PATH, timeout=10)
    sink_rows = sink_db.execute(
        "SELECT rowid, event_id, user_id, seq, processed_at FROM processed"
    ).fetchall()
    duplicate_stats = sink_db.execute(
        "SELECT consumer_pod, duplicates FROM dup_stats"
    ).fetchall()

    dlt_events = {}
    invalid_dlt = 0
    dump_lines = DLT_DUMP_PATH.read_text(encoding="utf-8").splitlines() if DLT_DUMP_PATH.exists() else []
    for line in dump_lines:
        try:
            event = json.loads(line.split("\t")[-1])
            dlt_events[event["event_id"]] = event
        except (ValueError, KeyError, TypeError):
            invalid_dlt += 1

    sequences_by_user = defaultdict(set)
    rows_by_user = defaultdict(list)
    for rowid, _event_id, user_id, seq, processed_at in sink_rows:
        sequences_by_user[user_id].add(seq)
        rows_by_user[user_id].append((parse_timestamp(processed_at), rowid, seq))
    for event in dlt_events.values():
        sequences_by_user[event["user_id"]].add(event["seq"])

    missing_by_user = {}
    for user_id, seq_set in sequences_by_user.items():
        expected = set(range(1, max(seq_set) + 1))
        missing = sorted(expected - seq_set)
        if missing:
            missing_by_user[user_id] = missing

    inversions = {}
    for user_id, rows in rows_by_user.items():
        max_seen_seq = 0
        count = 0
        for _timestamp, _rowid, seq in sorted(rows):
            if seq < max_seen_seq:
                count += 1
            else:
                max_seen_seq = seq
        if count:
            inversions[user_id] = count

    poison_ids_in_dlt = {
        event["event_id"]
        for event in dlt_events.values()
        if event.get("payload", {}).get("poison")
    }
    sink_event_ids = {row[1] for row in sink_rows}
    poison_in_sink = sorted(poison_ids_in_dlt & sink_event_ids)

    print(f"sink: {len(sink_rows)} событий, DLT: {len(dlt_events)} событий (битых строк дампа: {invalid_dlt})")
    print()

    if missing_by_user:
        print(f"1. Пропуски seq: {len(missing_by_user)} пользователей из {len(sequences_by_user)}")
        for user_id, missing in list(missing_by_user.items())[:DETAIL_LIMIT]:
            print(f"   user {user_id}: нет seq {', '.join(map(str, missing))}")
        if len(missing_by_user) > DETAIL_LIMIT:
            print(f"   ... и ещё {len(missing_by_user) - DETAIL_LIMIT} пользователей")
    else:
        print("1. Пропуски seq: нет")

    dup_total = sum(count for _, count in duplicate_stats)
    if dup_total:
        breakdown = ", ".join(f"{pod}: {count}" for pod, count in duplicate_stats)
        print(f"2. Дублей отброшено дедупликацией: {dup_total} ({breakdown})")
    else:
        print("2. Дублей отброшено дедупликацией: 0")

    inv_total = sum(inversions.values())
    print(f"3. Инверсий порядка: {inv_total}")
    for user_id, count in list(inversions.items())[:DETAIL_LIMIT]:
        print(f"   user {user_id}: {count}")
    if len(inversions) > DETAIL_LIMIT:
        print(f"   ... и ещё {len(inversions) - DETAIL_LIMIT} пользователей")

    print(f"4. Ядовитые события в sink: {len(poison_in_sink) if poison_in_sink else 'нет'}")

    has_failures = bool(missing_by_user or inversions or poison_in_sink)
    print()
    print("RESULT:", "FAIL" if has_failures else "PASS")
    return int(has_failures)


if __name__ == "__main__":
    sys.exit(main())