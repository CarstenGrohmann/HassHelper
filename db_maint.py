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
6. Cleanup old sensors in Home Assistant

Example: Assign data of sensor.deye_inverter_mqtt_phase1_voltage to sensor.deye_inverter_mqtt_grid_l1_voltage

# ./db_maint.py merge_check_single_sensor sensor.deye_inverter_mqtt_phase1_voltage sensor.deye_inverter_mqtt_grid_l1_voltage
    INFO: merge_check_single_sensor(): Old sensor id: 101
    INFO: merge_check_single_sensor(): New sensor id: 172
    INFO: merge_check_single_sensor(): Consistency checks passed
    INFO: merge_check_single_sensor(): Assign data from the old sensor in table statistics to the new sensor
    INFO: exec_modify(): Dry-run:
UPDATE statistics
SET metadata_id = :new_sensor_id
WHERE metadata_id = :old_sensor_id;
 with parameter {'table': 'statistics', 'new_sensor_id': 172, 'old_sensor_id': 101}
    INFO: merge_check_single_sensor(): Assign data from the old sensor in table statistics_short_term to the new sensor
    INFO: exec_modify(): Dry-run:
UPDATE statistics_short_term
SET metadata_id = :new_sensor_id
WHERE metadata_id = :old_sensor_id;
 with parameter {'table': 'statistics_short_term', 'new_sensor_id': 172, 'old_sensor_id': 101}
    INFO: merge_check_single_sensor(): All data from the old sensor assigned to the new sensor

# ./db_maint.py -m merge_check_single_sensor sensor.deye_inverter_mqtt_phase1_voltage sensor.deye_inverter_mqtt_grid_l1_voltage
    INFO: merge_check_single_sensor(): Old sensor id: 101
    INFO: merge_check_single_sensor(): New sensor id: 172
    INFO: merge_check_single_sensor(): Consistency checks passed
    INFO: merge_check_single_sensor(): Assign data from the old sensor in table statistics to the new sensor
5257 rows modified / deleted
    INFO: merge_check_single_sensor(): Assign data from the old sensor in table statistics_short_term to the new sensor
8137 rows modified / deleted
    INFO: merge_check_single_sensor(): All data from the old sensor assigned to the new sensor

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


def exec_modify(stmt: str, params: tuple[str] | dict[str, int | str] = ()) -> None:
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


def exec_select(stmt: str, params: tuple | dict[str, int | str] = ()):
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


def list_all_sensors():
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


def merge_all_sensors():
    """\
    Search all sensor pairs consist of "<name>" and "<name>_2" and merge data
    back to the "<name>" sensor.

    See: "merge_single" option resp. merge_single_sensor() function about
         limitations and how it works.
    """
    stmt = "SELECT statistic_id FROM statistics_meta WHERE statistic_id like '%_2';"
    cursor = exec_select(stmt)
    list_all_2_sensors = [x[0] for x in cursor.fetchall()]
    sensors2merge = []
    for sensor2_name in list_all_2_sensors:
        sensor_name = sensor2_name[:-2]
        if sensor_name.startswith("sensor.electricmeter"):
            logging.info("Skip electric meter sensor %s", sensor_name)
            continue
        logging.info("Process sensor %s", sensor_name)
        sensors2merge.append(sensor_name)

    for sensor_name in sensors2merge:
        merge_single_sensor(sensor_name)


def merge_single_sensor(original_name: str):
    """\
    Assign sensor data from the "<name>_2" sensor to the "<original name>" sensor
    and delete "<name>_2" sensors.

    This function doesn't do any condition checks.

    It changes the reference of all items in the statistics tables and states table
    from the "<name>_2" sensor to the "<original name>" sensor and deletes the
    "<name>_2" sensor from the statistics_meta and states_meta tables.
    """
    sensor2_name = f"{original_name}_2"

    if original_name.endswith("_2"):
        logging.error(
            "Sensor name %s ends with _2. This is not allowed. Use the original sensor name.",
            original_name,
        )
        return
    if query_statistics_sensor_id(original_name) is None:
        logging.warning(
            "Sensor %s does not exist. Skip changing data assignment of this sensor.",
            original_name,
        )
        return
    if query_statistics_sensor_id(sensor2_name) is None:
        logging.warning(
            "Sensor %s does not exist. Skip changing data assignment of this sensor.",
            sensor2_name,
        )
        return

    logging.info("Assign values from %s to %s", sensor2_name, original_name)
    for table in ["statistics", "statistics_short_term"]:
        logging.info('Update table "%s"', table)
        exec_modify(
            f"""
            UPDATE OR IGNORE {table}
              SET metadata_id = (SELECT id FROM statistics_meta WHERE statistic_id = :original_name LIMIT 1)
              WHERE metadata_id = (SELECT id FROM statistics_meta WHERE statistic_id = :sensor2_name LIMIT 1);
            """,
            {"original_name": original_name, "sensor2_name": sensor2_name},
        )

    logging.info("Delete statistics meta data for %s", sensor2_name)
    exec_modify(
        "DELETE FROM statistics_meta WHERE statistic_id = ?",
        (sensor2_name,),
    )

    logging.info('Update table "states"')
    exec_modify(
        """
        UPDATE OR IGNORE states
          SET metadata_id = (SELECT metadata_id FROM states_meta WHERE entity_id = :original_name LIMIT 1)
          WHERE metadata_id = (SELECT metadata_id FROM states_meta WHERE entity_id = :sensor2_name LIMIT 1);
        """,
        {"original_name": original_name, "sensor2_name": sensor2_name},
    )
    logging.info("Delete states meta data for %s", sensor2_name)
    exec_modify("DELETE FROM states_meta WHERE entity_id = ?", (sensor2_name,))

    if dry_run:
        logging.info(
            "This was a dry-run. No data was modified. Run script with -m to modify the database."
        )
    else:
        logging.info(
            "All data from the old sensor %s assigned to the new sensor %s",
            sensor2_name,
            original_name,
        )


def merge_check_single_sensor(old_sensor_name: str, new_sensor_name: str):
    """\
    Assign sensor data from old sensor to new sensor

    This function does some condition checks before assigning the data.
    It checks:
    - if the old and the new sensor exist
    - if the old and the new sensor have different ids
    - if the last items of the old sensor are older than the first items of the new sensor

    After the checks passed, the function assigns the data from the old sensor to the new
    sensor in both statistics tables.

    The old sensor will not be deleted.
    """

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
            SET metadata_id = {new_sensor_id}
            WHERE metadata_id = {old_sensor_id};
            """,
        )
    if dry_run:
        logging.info(
            "This was a dry-run. No data was modified. Run script with -m to modify the database."
        )
    else:
        logging.info("All data from the old sensor assigned to the new sensor")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Performing sensor maintenance tasks in HASS SQLite database",
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

    subparsers = parser.add_subparsers(
        description="Available commands",
        dest="action",
        help="sub-command -h|--help for detailed help on each command",
    )
    list_parser = subparsers.add_parser(
        "list_sensors",
        description="Show all available sensors",
        help="Show all available sensors",
    )
    merge_check_parser = subparsers.add_parser(
        "merge_check",
        description=merge_check_single_sensor.__doc__,
        help='Merge items from one sensor to another sensor with some checks.',
    )
    merge_check_parser.add_argument(
        dest="sensor_name_old",
        help="name old sensor",
    )
    merge_check_parser.add_argument(
        dest="sensor_name_new",
        help="name new sensor",
    )
    desc = 'Assign sensor data from all "<name>_2" sensors to original sensors and delete "<name>_2" sensors, as they were created by mistake.'
    merge_all_parser = subparsers.add_parser(
        "merge_all",
        #     The "<name>_2" sensors were created by mistake.
        description=merge_all_sensors.__doc__,
        help='Search and cleanup all "<name>_2" sensors, as they were created by mistake.',
    )
    merge_single_parser = subparsers.add_parser(
        "merge_single",
        description=merge_single_sensor.__doc__,
        help='Merge items from "<name>_2" sensor back to "<name>" sensor w/o checks.',
    )
    merge_single_parser.add_argument(
        dest="sensor_name",
        help="Original sensor name (without _2)",
    )

    args = parser.parse_args()

    logger = logging.getLogger()
    FORMAT = "%(levelname)8s: %(funcName)s(): %(message)s"
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter(FORMAT))
    logger.addHandler(handler)
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

    if args.action == "list_sensors":
        list_all_sensors()
    elif args.action == "merge_check":
        merge_check_single_sensor(args.sensor_name_old, args.sensor_name_new)
    elif args.action == "merge_all":
        merge_all_sensors()
    elif args.action == "merge_single":
        merge_single_sensor(args.sensor_name)
