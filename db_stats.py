#!/usr/bin/python

"""
Display statistics about a Home Assistant SQLite database.

Copyright (c) 2026 Carsten Grohmann
License: MIT (see LICENSE.txt)
THIS PROGRAM COMES WITH NO WARRANTY
"""

import argparse
import os.path
import sqlite3
import sys
from typing import Optional

conn: Optional[sqlite3.Connection] = None
"""Database connection"""


def exec_select(stmt: str, params: tuple[str, ...] | dict[str, int | str] = ()):
    """Execute a SELECT statement and return the cursor."""
    return conn.execute(stmt, params)


def print_section(title: str) -> None:
    print(f"\n=== {title} ===")


def print_table(
    headers: list[str],
    rows: list[tuple],
    col_align: Optional[list[str]] = None,
) -> None:
    """Print rows as an aligned table with column headers."""
    widths = [len(h) for h in headers]
    for row in rows:
        for i, cell in enumerate(row):
            widths[i] = max(widths[i], len(str(cell)))

    if col_align is None:
        col_align = ["<"] * len(headers)

    fmt_row = lambda row: "  ".join(
        f"{str(cell):{col_align[i]}{widths[i]}}" for i, cell in enumerate(row)
    )
    print(fmt_row(headers))
    print("  ".join("-" * w for w in widths))
    for row in rows:
        print(fmt_row(row))


def print_db_info(db_filename: str) -> None:
    print_section("Database")
    size = os.path.getsize(db_filename)
    print(f"File:  {db_filename}")
    print(f"Size:  {size / (1024 * 1024):.1f} MB ({size:,} bytes)")


def print_table_counts() -> None:
    print_section("Tables")
    tables = [
        r[0]
        for r in exec_select(
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
        ).fetchall()
    ]
    rows = [
        (table, f"{exec_select(f'SELECT COUNT(*) FROM {table}').fetchone()[0]:,}")
        for table in tables
    ]
    print_table(["Table", "Rows"], rows, ["<", ">"])


def print_sensor_summary() -> None:
    print_section("Sensors")
    stats = exec_select("SELECT COUNT(*) FROM statistics_meta").fetchone()[0]
    states = exec_select("SELECT COUNT(*) FROM states_meta").fetchone()[0]
    print(f"statistics_meta:  {stats:,}")
    print(f"states_meta:      {states:,}")


def print_per_sensor_stats() -> None:
    print_section("Per-Sensor Statistics")
    cursor = exec_select("""
        SELECT
            sm.statistic_id,
            (SELECT COUNT(*) FROM statistics s WHERE s.metadata_id = sm.id),
            (SELECT COUNT(*) FROM statistics_short_term sst WHERE sst.metadata_id = sm.id)
        FROM statistics_meta sm
        ORDER BY sm.statistic_id
    """)
    rows = [(r[0], f"{r[1]:,}", f"{r[2]:,}") for r in cursor.fetchall()]
    print_table(["Sensor", "statistics", "short_term"], rows, ["<", ">", ">"])


def print_states_per_sensor() -> None:
    print_section("States per Sensor")
    cursor = exec_select("""
        SELECT
            sm.entity_id,
            COUNT(*) AS cnt,
            COUNT(*) * 100 / (SELECT COUNT(*) FROM states) AS pct
        FROM states
        INNER JOIN states_meta sm ON states.metadata_id = sm.metadata_id
        GROUP BY sm.entity_id
        ORDER BY cnt DESC
    """)
    rows = [(r[0], f"{r[1]:,}", f"{r[2]}%") for r in cursor.fetchall()]
    print_table(["Sensor", "States", "%"], rows, ["<", ">", ">"])


def print_event_types() -> None:
    print_section("Event Types")
    cursor = exec_select("""
        SELECT
            et.event_type,
            COUNT(*) AS cnt,
            COUNT(*) * 100 / (SELECT COUNT(*) FROM events) AS pct
        FROM events
        INNER JOIN event_types et ON events.event_type_id = et.event_type_id
        GROUP BY et.event_type
        ORDER BY cnt DESC
    """)
    rows = [(r[0], f"{r[1]:,}", f"{r[2]}%") for r in cursor.fetchall()]
    print_table(["Event Type", "Count", "%"], rows, ["<", ">", ">"])


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Display statistics about a Home Assistant SQLite database",
    )
    parser.add_argument(
        "-d",
        "--db-file",
        default="config/home-assistant_v2.db",
        dest="db_filename",
        help="SQLite database file",
    )
    args = parser.parse_args()

    if not os.path.isfile(args.db_filename):
        print(
            f"ERROR: Database file {args.db_filename} not found. "
            "Set existing database file with option -d / --db-file.",
            file=sys.stderr,
        )
        sys.exit(1)

    conn = sqlite3.connect(args.db_filename)

    print_db_info(args.db_filename)
    print_table_counts()
    print_sensor_summary()
    print_per_sensor_stats()
    print_states_per_sensor()
    print_event_types()
