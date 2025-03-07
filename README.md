# Home Assistant Helper

## Summary
These scripts help to maintain [Home Assistant](https://www.home-assistant.io).

All these scripts should be adapted to the user's needs, and the generated
SQL file should be checked.

A test with a copy of the Home Assistant database outside HA is recommended.

### copy_template_data_from_states_to_statistics.sh

If you set up a template sensor without a state class, the historical
data is stored in the table "states" and will be irretrievably deleted
after a certain time.

To keep the historical data, you need to set the state class property
and copy the data from the table "states" to the statistics table, as the
statistics table is used to store historical data.

This script helps to copy sensor data from the table "states" to the
statistics table in an SQLite database.

## restore_old_sensor_data_from_backup.sh

The script is intended to help users who purged sensor data by accident.

This script restores old databases from backup snapshots, extracts sensor
data from the short-term and long-term statistics tables, and prepares
SQL statements to re-insert.

Additionally, the script can be used to map old sensor IDs to new sensor IDs.

## License

These scripts are licensed under the MIT license.

> Copyright (c) 2025 Carsten Grohmann,  mail &lt;add at here&gt; carstengrohmann.de
>
> Permission is hereby granted, free of charge, to any person obtaining a copy of
> this software and associated documentation files (the "Software"), to deal in
> the Software without restriction, including without limitation the rights to
> use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies
> of the Software, and to permit persons to whom the Software is furnished to do
> so, subject to the following conditions:
>
> The above copyright notice and this permission notice shall be included in all
> copies or substantial portions of the Software.
>
> THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
> IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
> FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
> AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
> LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
> OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
> SOFTWARE.

Enjoy!

Carsten Grohmann
