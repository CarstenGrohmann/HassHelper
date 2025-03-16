#!/usr/bin/python

"""
This script performs some maintenance tasks on a Home Assistant SQLite
database.

The script is designed to be run in dry-run mode by default. To actually
modify the database, the "--modify" option must be set.

A test with a copy of the Home Assistant database outside HA is recommended.

Usage:
1. Run the script in dry-run mode to see what would be done
2. Shutdown Home Assistant
3. Create a backup of your database
4. Run the script with the "--modify" option
5. Restart Home Assistant and check sensor data

Copyright (c) 2025 Carsten Grohmann
License: MIT (see LICENSE.txt)
THIS PROGRAM COMES WITH NO WARRANTY
"""

import argparse
import logging
import os.path
import sqlite3
import sys
import textwrap
from typing import Optional

conn: Optional[sqlite3.Connection] = None
"""Database connection"""

dry_run: bool = True
"""Don't modify the database if True"""


def exec_modify(stmt: str, params: [tuple | dict[str, int | str]] = ()) -> None:
    """Execute a UPDATE, DELETE or INSERT statement."""
    with conn:
        stmt = textwrap.dedent(stmt)
        if dry_run:
            logging.info("Dry-run: %s with parameter %s", stmt, params)
            return
        try:
            cursor = conn.execute(stmt, params)
        except:
            logging.error(
                "Error executing statement %s with parameter %s", stmt, params
            )
            raise
        print(f"{cursor.rowcount} rows modified / deleted")


def exec_select(stmt: str, params: [tuple | dict[str, int | str]] = ()):
    """Execute a SELECT statement and return the cursor."""
    with conn:
        stmt_wo_leading_whitespaces = textwrap.dedent(stmt).strip()
        assert stmt_wo_leading_whitespaces.startswith(
            "SELECT"
        ), f"Empty SELECT statement after trimming whitespaces:\n{stmt_wo_leading_whitespaces}"
        try:
            cursor = conn.execute(stmt_wo_leading_whitespaces, params)
        except:
            logging.error(
                "Error executing statement %s with parameter %s", stmt, params
            )
            raise
    return cursor


def query_statistics_sensor_id(sensor_name: str) -> Optional[int]:
    """
    Return the sensor id for a given sensor name or None if the sensor does
    not exist.
    """
    res = exec_select(
        """
        SELECT id, statistic_id
        FROM statistics_meta
        WHERE statistic_id = ?
        """,
        (sensor_name,),
    )
    rows = res.fetchall()
    if len(rows) == 0:
        logging.error("Sensor %s not found.", sensor_name)
        return None
    if len(rows) > 1:
        logging.error("Multiple sensors with name %s found.", sensor_name)
        return None

    return rows[0][0]


def list_sensors():
    """List all known sensor names"""
    res = exec_select(
        """SELECT statistic_id as a FROM statistics_meta WHERE statistic_id like '%sensor%'
           UNION
           SELECT entity_id as a FROM states_meta WHERE entity_id like '%sensor%'
           ORDER BY a ASC;
        """,
    )
    logging.info("Available sensors:")
    for name in res.fetchall():
        logging.info(" - %s", name[0])


def move_data(old_sensor_name: str, new_sensor_name: str):
    """Assign sensor data from old sensor to new sensor"""

    # error messages are already logged in query_statistics
    old_sensor_id = query_statistics_sensor_id(old_sensor_name)
    if old_sensor_id is None:
        return
    logging.info("Old sensor id: %d", old_sensor_id)
    new_sensor_id = query_statistics_sensor_id(new_sensor_name)
    if new_sensor_id is None:
        return
    logging.info("New sensor id: %d", new_sensor_id)

    if old_sensor_id == new_sensor_id:
        logging.error("Old and new sensor have the same id %d", old_sensor_id)
        return

    # Check that the last dates of the old sensor are older than the first
    # date of the new sensor
    cursor = exec_select(
        """
        SELECT created_ts, start_ts
        FROM statistics
        WHERE metadata_id = :sensor_id
        ORDER BY created_ts DESC
        LIMIT 1
        """,
        {"sensor_id": old_sensor_id},
    )
    old_created_ts, old_start_ts = cursor.fetchone()

    cursor = exec_select(
        """
        SELECT created_ts, start_ts
        FROM statistics
        WHERE metadata_id = :sensor_id
        ORDER BY created_ts ASC
        LIMIT 1
        """,
        {"sensor_id": new_sensor_id},
    )
    new_created_ts, new_start_ts = cursor.fetchone()

    if new_created_ts < old_created_ts:
        logging.error(
            "First created_ts timestamp %s of the new sensor %s is older than "
            "the last timestamp %s of the old sensor %s",
            new_created_ts,
            new_sensor_name,
            old_created_ts,
            old_sensor_name,
        )
        return
    if new_created_ts == old_created_ts:
        logging.error(
            "First created_ts timestamp %s of the new sensor %s is equal to "
            "the last timestamp %s of the old sensor %s",
            new_created_ts,
            new_sensor_name,
            old_created_ts,
            old_sensor_name,
        )
        return
    if new_start_ts < old_start_ts:
        logging.error(
            "First start_ts timestamp %s of the new sensor %s is older than "
            "the last timestamp %s of the old sensor %s",
            new_start_ts,
            new_sensor_name,
            old_start_ts,
            old_sensor_name,
        )
        return
    if new_start_ts == old_start_ts:
        logging.error(
            "First start_ts timestamp %s of the new sensor %s is equal to "
            "the last timestamp %s of the old sensor %s",
            new_start_ts,
            new_sensor_name,
            old_start_ts,
            old_sensor_name,
        )
        return
    logging.info("Consistency checks passed")

    for t in ("statistics", "statistics_short_term"):
        logging.info("Assign data from the old sensor in table %s to the new sensor", t)
        exec_modify(
            f"""
            UPDATE {t}
            SET metadata_id = :new_sensor_id
            WHERE metadata_id = :old_sensor_id;
            """,
            {
                "table": t,
                "new_sensor_id": new_sensor_id,
                "old_sensor_id": old_sensor_id,
            },
        )
    logging.info("All data from the old sensor assigned to the new sensor")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Modify the HASS SQLite database",
        epilog=(
            "By default, the script runs in dry-run mode. Set option -m "
            "resp. --modify to change the database. Exit the Home Assistant "
            "beforehand and then restart it. A database backup is "
            "recommended to be able to undo the change in case of unexpected "
            "results."
        ),
    )
    parser.add_argument(
        "-m",
        "--modify",
        action="store_false",
        default=True,
        dest="dry_run",
        help="Modify the database",
    )
    parser.add_argument(
        "-d",
        "--db-file",
        default="config/home-assistant_v2.db",
        dest="db_filename",
        help="SQLite database file",
    )

    subparsers = parser.add_subparsers(description="Available commands", dest="action")
    md_parser = subparsers.add_parser(
        "move_data",
        description="Assign data from old sensor to new sensor. This "
        "command can be used to update statistical data after "
        "a sensor has be renamed.",
        help="Assign data from old sensor to new sensor",
    )
    md_parser.add_argument(
        dest="sensor_name_old",
        help="name old sensor",
    )
    md_parser.add_argument(
        dest="sensor_name_new",
        help="name new sensor",
    )

    list_parser = subparsers.add_parser(
        "list_sensors",
        description="Show all available sensors",
        help="Show all available sensors",
    )

    args = parser.parse_args()

    logger = logging.getLogger()
    FORMAT = "%(levelname)8s: %(funcName)s(): %(message)s"
    logging.basicConfig(format=FORMAT)
    logger.setLevel(logging.INFO)

    if not args.action:
        parser.print_help()
        sys.exit(1)

    dry_run = args.dry_run

    if not os.path.isfile(args.db_filename):
        logging.error(
            "Database file %s not found. Set existing database file with "
            "option -d / --db-file.",
            args.db_filename,
        )
        sys.exit(1)

    conn = sqlite3.connect(args.db_filename)
    conn.set_trace_callback(logging.debug)
    try:
        conn.autocommit = False
    except AttributeError:
        conn.isolation_level = None

    if args.action == "move_data":
        move_data(args.sensor_name_old, args.sensor_name_new)
    elif args.action == "list_sensors":
        list_sensors()
