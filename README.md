# nagios-check-plugin-influxdb

A simple nagios plugin to query an InfluxDB database.

## Example usage

	check_influxdb_query.py --host localhost --port 8086 --user mon_user --password mon_password --schema system --query "SELECT mean(value) FROM cpu WHERE time > now() - 5m GROUP BY time(1m)" --warning='0:85' --critical='0:90' --output-template='High CPU usage (%s%%)'

## Details

Group by tags are supported, every returned series would get tested against threshold values.

Selecting a range of time is also supported. All the values outside thresholds are returned in the text output part. Only the value farthest away to the threshold range is returned in the perf data part (one for each series).

## Dependencies

The official InfluxDB python plugin is required: [influxdata/influxdb-python](https://github.com/influxdata/influxdb-python).
