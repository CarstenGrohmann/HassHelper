#!/bin/bash
#
# Extract old statistics data from backup snapshots and prepare SQL
# statements to re-insert the data with modified sensor IDs.
#
# Copyright (c) 2025 Carsten Grohmann
# License: MIT (see LICENSE.txt)
# THIS PROGRAM COMES WITH NO WARRANTY

# DESCRIPTION
# ===========
#
# This script restores old databases from backup snapshots, extracts sensor
# data from the short-term and long-term statistics tables, and prepares
# SQL statements to re-insert.
#
# The script is intended to help users who purged sensor data by accident.
#
# Additionally, the script can be used to map old sensor IDs to new sensor
# IDs.
#
# The script should be adapted to the user's needs, and the generated SQL
# file should be checked. A test with a copy of the Home Assistant database
# outside HA is recommended.

# REQUIREMENTS
# ============
# * bash version 4 or newer
# * sqlite3 command line tool
#
# USAGE
# =====
#  1. Read and understand this script
#  2. Adapt this file to your needs, check "CHANGEME" comments
#  3. Run this script
#  4. Check script output
#  5. Check the generated SQL file
#  6. Shutdown Home Assistant
#  7. Create a backup of your database
#  8. Run the generated SQL file
#    $ sqlite3 config/home-assistant_v2.db < insert_statistics.sql
#    $ sqlite3 config/home-assistant_v2.db < insert_statistics_short_term.sql
#  9. Restart Home Assistant and check sensor data
#
# ROLLBACK
# ========
# Restore your database from backup

# Directory to restore backup snapshots inside
RESTORE_DIR="./restore"

# Names of the tables to extract sensor data from
TABLES=(
  "statistics"
  "statistics_short_term"
)

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
    read -p "Press <enter> to continue ..."
  fi
  ((i++))
done

echo
echo "Extract sensor data"
echo "==================="
for snap in ${LIST_BACKUP_IDS[@]}; do
   SENSOR_DATA_FILE="$RESTORE_DIR/${snap}/sensors.txt"
   if [[ -s "$SENSOR_DATA_FILE" ]]; then
     echo "Sensor data for snapshot $snap already extracted - ignoring"
     continue
  fi
  echo "Extract sensor data from SQLite DB in snapshot ${snap}"
  sqlite3 "$RESTORE_DIR/${snap}/home-assistant_v2.db" --noheader " \
    SELECT * FROM statistics_meta WHERE statistic_id like 'sensor.self_used_solar_power_%' \
    " > "$SENSOR_DATA_FILE"
done

echo
echo "Check for equal sensor ids"
echo "=========================="
LAST=""
for snap in ${LIST_BACKUP_IDS[@]}; do
   echo "Compare sensor data between $LAST and $snap"
   CURR="$RESTORE_DIR/${snap}/sensors.txt"
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

for table in ${TABLES[@]}; do
  echo
  echo "Check for equal schema of table ${table}"
  echo "========================================"
  LAST=""
  for snap in ${LIST_BACKUP_IDS[@]}; do
     SCHEMA_FILE="$RESTORE_DIR/${snap}/schema_${table}.txt"
     if [[ -s "$SCHEMA_FILE" ]]; then
       echo "Schema for snapshot $snap already extracted - ignoring"
       continue
    fi
     echo "Compare schema of ${table} between $LAST and $snap"
     sqlite3 "$RESTORE_DIR/${snap}/home-assistant_v2.db" ".schema ${table}" > ${SCHEMA_FILE}
     if [[ $LAST ]]; then
       diff -wu "$LAST" "$SCHEMA_FILE" > /dev/null 2>&1
       res=$?
       if [[ $res -ne 0 ]]; then
         echo "Schema of table ${table} differs between $LAST (old) and $SCHEMA_FILE (current)"
         diff -wu "$LAST" "$SCHEMA_FILE"
       else
         echo "no changes"
       fi
     fi
     LAST="$SCHEMA_FILE"
  done
done

echo
echo "Extract data from statistics tables"
echo "==================================="
for table in ${TABLES[@]}; do
  for snap in ${LIST_BACKUP_IDS[@]}; do
     DATA_FILE="$RESTORE_DIR/${snap}/data_${table}.sql"
     if [[ -s "$DATA_FILE" ]]; then
       echo "Statistics data from table ${table} in snapshot ${snap} already extracted - ignoring"
       continue
    fi
     echo "Extract statistics data from table ${table} in snapshot ${snap}"
     # CHANGEME: Replace ids (35 till 41) with old sensor IDs
     sqlite3 "$RESTORE_DIR/${snap}/home-assistant_v2.db" -quote "\
       SELECT * FROM ${table} WHERE metadata_id in (35,36,37,38,39,40,41); \
       " > ${DATA_FILE}
  done
done

echo
echo "Merge data"
echo "=========="
for table in ${TABLES[@]}; do
  TOTAL_FILE="./total_${table}.sql"
   if [[ -s "$TOTAL_FILE" ]]; then
     echo "Merged data already exists - ignoring"
     continue
  fi
  rm "${TOTAL_FILE}"
  for snap in ${LIST_BACKUP_IDS[@]}; do
     echo "Merge table ${table} from snapshot ${snap}"
     DATA_FILE="$RESTORE_DIR/${snap}/data_${table}.sql"
     cat "${DATA_FILE}" >> "${TOTAL_FILE}"
  done
  sort -gk1 -t "|" -u "${TOTAL_FILE}" > "${TOTAL_FILE}.sorted"
  mv "${TOTAL_FILE}.sorted" "${TOTAL_FILE}"
done

echo
echo "Prepare SQL statements and map sensor IDs"
echo "========================================="
for table in ${TABLES[@]}; do
  TOTAL_FILE="./total_${table}.sql"
  SQL_FILE="./insert_${table}.sql"
  awk -vtable=${table} -F "|" '{
    c_id = ($1 == "" ? "NULL" : $1);
    c_created = ($2 == "" ? "NULL" : $2);
    c_created_ts = ($3 == "" ? "NULL" : $3);
    c_metadata_id = ($4 == "" ? "NULL" : $4);
    c_start = ($5 == "" ? "NULL" : $5);
    c_start_ts = ($6 == "" ? "NULL" : $6);
    c_mean = ($7 == "" ? "NULL" : $7);
    c_min = ($8 == "" ? "NULL" : $8);
    c_max = ($9 == "" ? "NULL" : $9);
    c_last_reset = ($10 == "" ? "NULL" : $10);
    c_last_reset_ts = ($11 == "" ? "NULL" : $11);
    c_state = ($12 == "" ? "NULL" : $12);
    c_sum = ($13 == "" ? "NULL" : $13);
    # CHANGEME: Update mapping between old and new sensor IDs
    switch (c_metadata_id) {
      #  -5|sensor.daily_production|recorder|kWh|0|1|
      # +99|sensor.deye_inverter_mqtt_production_today|recorder|kWh|0|1|
      case 5:
        c_metadata_id = 99;
        break;

      #    6|sensor.daily_production_1|recorder|kWh|0|1|
      # +109|sensor.deye_inverter_mqtt_pv1_production_today|recorder|kWh|0|1|
      case 6:
        c_metadata_id = 109;
        break;

      #   -8|sensor.total_production|recorder|kWh|0|1|
      # +100|sensor.deye_inverter_mqtt_production_total|recorder|kWh|0|1|
      case 8:
        c_metadata_id = 100;
        break;

      #    9|sensor.total_production_1|recorder|kWh|0|1|
      # +110|sensor.deye_inverter_mqtt_pv1_total|recorder|kWh|0|1|
      case 9:
        c_metadata_id = 110;
        break;

      # take over as it is:
      #  35|sensor.electricmeter_energy_in_total|recorder|kWh|0|1|
      #  36|sensor.electricmeter_energy_in_tariff_1|recorder|kWh|0|1|
      case 35:
      case 36:
        break;

      #  -37|sensor.electricmeter_energy_in_tariff_2|recorder|kWh|0|1|
      # +147|sensor.electricmeter_energy_in_tariff_2|recorder|kWh|0|1|
      case 37:
        c_metadata_id = 147;
        break;

      # take over as it is:
      #  38|sensor.electricmeter_energy_out_total|recorder|kWh|0|1|
      #  39|sensor.electricmeter_energy_out_tariff_1|recorder|kWh|0|1|
      # take over as it is
      case 38:
      case 39:
        break;

      #  -40|sensor.electricmeter_energy_out_tariff_2|recorder|kWh|0|1|
      # +148|sensor.electricmeter_energy_out_tariff_2|recorder|kWh|0|1|
      case 40:
        c_metadata_id = 148;
        break;

      # take over as it is:
      # 41|sensor.electricmeter_current_active_power|recorder|W|1|0|
      case 41:
          break;

      # drop:
      # 7|sensor.daily_production_2|recorder|kWh|0|1|
      # 10|sensor.total_production_2|recorder|kWh|0|1|
      case 7:
      case 10:
        break;
    };
     print ("INSERT OR REPLACE INTO", table, \
      "(id, created, created_ts, metadata_id, start, start_ts, mean, min, max, last_reset, last_reset_ts, state, sum) " \
      "VALUES(" $1 ", " c_created ", " c_created_ts ", " c_metadata_id ", " c_start ", " c_start_ts ", " c_mean ", " \
      c_min ", " c_max ", " c_last_reset ", " c_last_reset_ts ", " c_state ", " c_sum ");" );
    }
    ' "${TOTAL_FILE}" > "${SQL_FILE}"
done

echo
echo "Script finished successfully"
echo "============================"
