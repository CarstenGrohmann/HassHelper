#!/bin/bash
#
# Copy sensor data from table "states" to statistics table
#
# Copyright (c) 2025 Carsten Grohmann
# License: MIT (see LICENSE.txt)
# THIS PROGRAM COMES WITH NO WARRANTY

# DESCRIPTION
# ===========
#
# If you set up a template sensor without a state class, the historical
# data is stored in the table "states" and will be irretrievably deleted
# after a certain time.
# To keep the historical data, you need to set the state class property
# and copy the data from the table "states" to the statistics table, as the
# statistics table is used to store historical data.
#
# This script helps to copies sensor data from the table "states" to the
# statistics table in an SQLite database.
#
# The script should be adapted to the user's needs, and the generated SQL
# file should be checked for warnings and errors before execution.

# REQUIREMENTS
# ============
# * bash version 4 or newer
# * sqlite3 command line tool
# * Python 3
#
# USAGE
# =====
# 1. Add state class to all affected templates
# 2. Read and understand this script
# 3. Adapt this file to your needs, check "CHANGEME" comments
# 4. Run this script
# 5. Check script output
# 6. Check the generated SQL file for warnings and errors
# 7. Shutdown Home Assistant
# 8. Create a backup of your database
# 9. Run the generated SQL file
#   $ sqlite3 config/home-assistant_v2.db "SELECT MAX(id) FROM statistics;"
#   357043
#
#   $ sqlite3 -bail config/home-assistant_v2.db < insert_states_yearly.sql
#   $ sqlite3 -bail config/home-assistant_v2.db < insert_states_monthly.sql
#   $ sqlite3 -bail config/home-assistant_v2.db < insert_states_weekly.sql
#   $ sqlite3 -bail config/home-assistant_v2.db < insert_states_daily.sql
#
#   $ sqlite3 config/home-assistant_v2.db "SELECT MAX(id) FROM statistics;"
#   358476
#
#
# ROLLBACK
# ========
#   $ sqlite3 config/home-assistant_v2.db "DELETE FROM statistics WHERE id > <first MAX> AND id <= <second MAX>;"
# or
#   Restore your database from backup
#
#
# OTHER HELPFUL PROJECTS
# ======================
#   * https://github.com/patrickvorgers/Home-Assistant-Import-Energy-Data/
#   * https://github.com/frenck/spook

# Directory to restore backup snapshots inside
RESTORE_DIR="./restore"

# Names of the tables to extract sensor data from
SENSOR_TABLES=(
  "states_meta"
  "statistics_meta"
)

# Data source table name
DATA_TABLE="states"

# Sensor IDs from states_meta table
declare -A SENSOR_IDS
# CHANGEME
SENSOR_IDS["yearly"]=313
SENSOR_IDS["monthly"]=312
SENSOR_IDS["weekly"]=311
SENSOR_IDS["daily"]=302

# Unix epoch timestamp in localtime until the sensor data should be
# extracted. The extracted data will have a timestamp smaller than
# this.
# CHANGEME
SENSOR_DATA_TILL=1737727210

# List of backup snapshots
LIST_BACKUP_IDS=(
    "ID backup 1"
    "ID backup 2"
    "ID backup 3"
)

echo "Restore all SQLite databases from backup"
echo "========================================"
mkdir -p "$RESTORE_DIR"
# CHANGEME: Add code to restore all required SQLite databases from backup
#           As result the SQLite database home-assistant_v2.db should be
#           located in "$RESTORE_DIR/<snapshot_id>/".
i=0
for snap in "${LIST_BACKUP_IDS[@]}"; do
    echo "Restore SQLite DB from snapshot ${snap} #$i"
    if [[ -s "$RESTORE_DIR/${snap}/home-assistant_v2.db" ]]; then
      echo "Snapshot $snap already restored - ignoring"
      ((i++))
      continue
   fi
   if [[ -d "$RESTORE_DIR/${snap}" ]]; then
     echo "Deleting old restore directory $RESTORE_DIR/${snap}"
     rm -rf "${RESTORE_DIR:?}/${snap:?}"
   fi
   # CHANGEME: Add code to restore the SQLite database from backup
   # <restore command" "$snap" <restore options>
   if [[ $? -ne 0 ]]; then
     echo "ERROR: Restoring snapshot $snap failed"
     exit 1
  fi
  if [[ $((i % 7)) -eq 0 ]]; then
    read -p "Press <enter> to continue ...";
  fi
  ((i++))
done

echo
echo "Extract sensor data"
echo "==================="
for table in "${SENSOR_TABLES[@]}"; do
  for snap in "${LIST_BACKUP_IDS[@]}"; do
     SENSOR_DATA_FILE="$RESTORE_DIR/${snap}/sensors_${table}.txt"
     if [[ -s "$SENSOR_DATA_FILE" ]]; then
       echo "Sensor data for snapshot $snap already extracted - ignoring"
       continue
    fi
    echo "Extract senor data from SQLite DB in snapshot ${snap}"
    if [[ $table = "states_meta" ]]; then
      column_id="entity_id"
    elif [[ $table = "statistics_meta" ]]; then
      column_id="statistic_id"
    fi
    sqlite3 "$RESTORE_DIR/${snap}/home-assistant_v2.db" --noheader " \
       SELECT * FROM ${table} WHERE ${column_id} like 'sensor.self_used_solar_power_%' \
       " > "$SENSOR_DATA_FILE"
  done
done

echo
echo "Check for equal sensor ids"
echo "=========================="
for table in "${SENSOR_TABLES[@]}"; do
  LAST=""
  for snap in "${LIST_BACKUP_IDS[@]}"; do
     echo "Compare sensor data between $LAST and $snap"
     CURR="$RESTORE_DIR/${snap}/sensors_${table}.txt"
     if [[ $LAST ]]; then
       diff -wu "$LAST" "$CURR" > /dev/null 2>&1
       res=$?
       if [[ $res -ne 0 ]]; then
         echo "Sensors differs between $LAST (old) and $CURR (current)"
         diff -wu "$LAST" "$CURR"
       fi
     fi
     LAST="$CURR"
  done
done

echo
echo "Check for equal schema of table ${DATA_TABLE}"
echo "========================================"
LAST=""
for snap in "${LIST_BACKUP_IDS[@]}"; do
   SCHEMA_FILE="$RESTORE_DIR/${snap}/schema_${DATA_TABLE}.txt"
   if [[ -s "$SCHEMA_FILE" ]]; then
     echo "Sensor data for snapshot $snap already extracted - ignoring"
     continue
  fi
   echo "Compare schema of ${DATA_TABLE} between $LAST and $snap"
   sqlite3 "$RESTORE_DIR/${snap}/home-assistant_v2.db" ".schema ${DATA_TABLE}" > "${SCHEMA_FILE}"
   if [[ $LAST ]]; then
     diff -wu "$LAST" "$SCHEMA_FILE" > /dev/null 2>&1
     res=$?
     if [[ $res -ne 0 ]]; then
       echo "Schema of table ${DATA_TABLE} differs between $LAST (old) and $SCHEMA_FILE (current)"
       diff -wu "$LAST" "$SCHEMA_FILE"
     else
       echo "no changes"
     fi
   fi
   LAST="$SCHEMA_FILE"
done

echo
echo "Extract data from statistics tables"
echo "==================================="
for sensor_name in "${!SENSOR_IDS[@]}"; do
  for snap in "${LIST_BACKUP_IDS[@]}"; do
    DATA_FILE="$RESTORE_DIR/${snap}/data_${DATA_TABLE}_${sensor_name}.sql"
    if [[ -s "$DATA_FILE" ]]; then
      echo "Statistics data from table ${DATA_TABLE} for sensor ${sensor_name} in snapshot ${snap} already extracted - ignoring"
      continue
    fi
    echo "Extract statistics data from table ${DATA_TABLE} for sensor ${sensor_name} in snapshot ${snap}"
    sqlite3 "$RESTORE_DIR/${snap}/home-assistant_v2.db" -quote "
      SELECT state, last_updated_ts, metadata_id
      FROM ${DATA_TABLE}
      WHERE (metadata_id = ${SENSOR_IDS[$sensor_name]} AND last_updated_ts < ${SENSOR_DATA_TILL}) AND
            state NOT IN ('unknown', 'unavailable') AND
            state NOT LIKE '%sensor%'
      ORDER BY metadata_id, last_updated_ts ASC;" > "${DATA_FILE}"
  done
done

echo
echo "Merge data"
echo "=========="
for sensor_name in "${!SENSOR_IDS[@]}"; do
  TOTAL_FILE="./total_${DATA_TABLE}_${sensor_name}.sql"
   if [[ -s "$TOTAL_FILE" ]]; then
     echo "Merged data for sensor ${sensor_name} already exists - skipping merge"
     continue
  fi
  for snap in "${LIST_BACKUP_IDS[@]}"; do
     echo "Merge table ${DATA_TABLE} from snapshot ${snap}"
     DATA_FILE="$RESTORE_DIR/${snap}/data_${DATA_TABLE}_${sensor_name}.sql"
     cat "${DATA_FILE}" >> "${TOTAL_FILE}"
  done
  sort -gk2 -t "," -u "${TOTAL_FILE}" > "${TOTAL_FILE}.sorted"
  mv "${TOTAL_FILE}.sorted" "${TOTAL_FILE}"
done

echo
echo "Prepare SQL statements and map sensor IDs"
echo "========================================="
for sensor_name in "${!SENSOR_IDS[@]}"; do
  TOTAL_FILE="./total_${DATA_TABLE}_${sensor_name}.sql"
  SQL_FILE="./insert_${DATA_TABLE}_${sensor_name}.sql"

  python - "${TOTAL_FILE}" "${SQL_FILE}" <<'EOF'
import datetime
import sys

# 1 hour in seconds
HOUR = 60 * 60

# mapping of metadata_id between table "states" and table "statistics"
# CHANGEME
metadata_id_states2statistics = {
    302: 165,  # sensor.self_used_solar_power_daily_reset
    311: 166,  # sensor.self_used_solar_power_weekly_reset
    312: 167,  # sensor.self_used_solar_power_monthly_reset
    313: 164,  # sensor.self_used_solar_power_yearly_reset
}

# mapping of sensor id to sensor name
# CHANGEME
metadata_id_2sensor = {
    165: "sensor.self_used_solar_power_daily_reset",
    166: "sensor.self_used_solar_power_weekly_reset",
    167: "sensor.self_used_solar_power_monthly_reset",
    164: "sensor.self_used_solar_power_yearly_reset",
}

# check for incomplete history data
initial_offset_checked: dict[int, bool] = {k: False for k in metadata_id_2sensor}

# list of timestamps for each sensor to avoid more than one record per hour
hourly_timestamps: dict[int, list[float]] = {k: [] for k in metadata_id_2sensor}

INFILE_NAME = sys.argv[1]
OUTFILE_NAME = sys.argv[2]

with open(OUTFILE_NAME, "w") as outfile:
    for i, line in enumerate(open(INFILE_NAME).readlines()):
        line = line.strip()
        if not line or line.startswith("--") or line.startswith("#"):
            continue
        try:
            items = line.split(",")
            state = float(items[0].strip("'"))
            timestamp = float(items[1].strip("'"))
            metadata_id = metadata_id_states2statistics[int(items[2].strip("'"))]
        except:
            print(f"-- ERROR: Ignoring wrongly formatted line: {line}")
            continue

        if not initial_offset_checked[metadata_id]:
            if state != 0.0:
                print(
                    f"-- WARNING: Sensor {metadata_id_2sensor[metadata_id]} starts with unexpected value {state} "
                    "instead of 0.0. The history data is probably incomplete.",
                    file=outfile,
                )
            initial_offset_checked[metadata_id] = True

        # Start time is the last full hour - reduce timestamp by the started hour
        start_measurement = timestamp - (timestamp % HOUR)

        # add only the first record of each hour
        if start_measurement in hourly_timestamps[metadata_id]:
            continue
        hourly_timestamps[metadata_id].append(start_measurement)

        # sum is the value since the last reset.
        # sum does not need to be calculated for self_used_solar_power_*, as
        # state increases continuously and sum draws exactly the same curve
        # when the delta is calculated. Therefore, sum can simply be taken
        # from state.

        print(
            f"-- sensor {metadata_id_2sensor[metadata_id]} #{metadata_id}: "
            f"{datetime.datetime.fromtimestamp(start_measurement)} - {state:.4f}",
           file=outfile,
        )

        # "INSERT OR IGNORE" to prevent "UNIQUE constraint failed" on a unique indes of metadata_id and start_ts.
        print(
            "INSERT OR IGNORE INTO statistics (state, sum, metadata_id, created_ts, start_ts) "
            f"VALUES ({state:.4f}, {state:.4f}, {metadata_id}, {start_measurement}, {start_measurement});",
            file=outfile,
        )
EOF
  echo "Update file $SQL_FILE written. Check file for comments with warnings and error messages."
done

echo
echo "Script finished successfully"
echo "============================"
