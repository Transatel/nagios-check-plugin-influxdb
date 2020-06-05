#!/usr/bin/python3

import argparse
from pprint import pprint
from influxdb import InfluxDBClient

# check_influxdb_query.py --host localhost --port 8086 --schema system --query "SELECT mean(value) FROM cpu WHERE time > now() - 5m GROUP BY time(1m)"

# -------------------------------------------------------------------------
# FUNCTIONS

def parse_nagios_threshold(nagios_threshold):
    low = None
    high = None
    invert = False

    if '@' == nagios_threshold[0]:
        invert = True
        nagios_threshold = nagios_threshold[1:]

    if ':' in nagios_threshold:
        low, high = nagios_threshold.split(':')
    else:
        low = 0
        high = nagios_threshold

    if low == '~':
        low = float("-inf")
    elif low == '':
        low = 0
    else:
        low = float(low)

    if high == '':
        high = float("inf")
    else:
        high = float(high)

    return {
        'low': low,
        'high': high,
        'invert': invert,
    }


def test_nagios_threshold(value, nagios_threshold):

    nt = parse_nagios_threshold(nagios_threshold)

    result = value < nt['low'] or value > nt['high']
    if nt['invert']:
        result = not result
    return result


def get_nagios_threshold_middle_point(nagios_threshold):
    if nagios_threshold is None:
        return 0
    nt = parse_nagios_threshold(nagios_threshold)
    if nt['low'] == float("-inf") and nt['high'] == float("inf"):
        return 0
    if nt['low'] == float("-inf"):
        return nt['high']
    if nt['high'] == float("inf"):
        return nt['low']
    return (nt['high'] - nt['low']) / 2


def get_farthest_point_from_thresholds(points, nagios_threshold):
    middle = get_nagios_threshold_middle_point(nagios_threshold)
    farthest = None
    farthest_delta = 0
    for p in points:
        delta = abs(middle - p)
        if delta > farthest_delta:
          farthest_delta = delta
          farthest = p
    return farthest


def serialize_influx_series_key(key):

    if key[1] is None:
        return 'None'

    tag_values = key[1].values()
    tag_values = '.'.join(tag_values)
    return str(tag_values)


# -------------------------------------------------------------------------
# ARGUMENTS

parser = argparse.ArgumentParser(description='Nagios check plugin to execute an InfluxDB query and compare returned values agains thresholds.')

# Input data: raw data in InfluxDB
parser.add_argument("--host", type=str, required=False, default='localhost',
                    help="InfluxDB host")
parser.add_argument("--port", type=int, required=False, default=8086,
                    help="InfluxDB host")
parser.add_argument("--user", type=str, required=False,
                    help="InfluxDB user")
parser.add_argument("--password", type=str, required=False,
                    help="InfluxDB password")
parser.add_argument("--ssl", action='store_const', const=True, required=False,
                    help="connect using SSL")

parser.add_argument("--schema", type=str, required=True,
                    help="InfluxDB schema / database from which to retrieve input time series")
parser.add_argument("--query", type=str, required=True,
                    help="InfluxDB query")

parser.add_argument("--warning", type=str, required=False,
                    help="warning thresholds, nagios format")
parser.add_argument("--critical", type=str, required=False,
                    help="ciritcal thresholds, nagios format")

parser.add_argument("--output-template", type=str, required=False, default="Values: %s",
                    help="Template for outputing message")

args = parser.parse_args()


# -------------------------------------------------------------------------
# QUERY

influx_client = InfluxDBClient(args.host, args.port, args.user, args.password, args.schema, ssl=args.ssl, verify_ssl=True)

resultset_list = influx_client.query(args.query)

# points = results.get_points()


# -------------------------------------------------------------------------
# PARSE OUTPUT

tmp_message_ok_dict = {}

is_warn = False
tmp_message_warn_dict = {}
is_crit = False
tmp_message_crit_dict = {}
current_timestamp=0

is_resultset_empty = True

for key, series in resultset_list.items():

    is_resultset_empty = False

    tmp_message_warn_list = []
    tmp_message_crit_list = []
    tmp_message_ok_list = []

    # print("-------------")
    for row in series:
        nb_fields = len(row.keys()) -1
        if nb_fields > 1:
            message = "Query returned " + str(nb_fields) + " fields instead of 1."
            nagios_status = 3
            nagios_status_desc = 'UNKNOWN'
            message = nagios_status_desc + " - " + message
            print("Query returned " + str(nb_fields) + " fields instead of 1.")
            exit(nagios_status)

        for column, value in row.items():
            if column == 'time':
                # current_timestamp = value
                continue

            # print(str(value))

            if value is None:
                continue

            is_point_crit = False
            is_point_warn = False
            if args.critical:
                is_point_crit = test_nagios_threshold(value, args.critical)
                if is_point_crit:
                    is_crit = True
                    tmp_message_crit_list.append(value)
            if not is_point_crit and args.warning:
                is_point_warn = test_nagios_threshold(value, args.warning)
                if is_point_warn:
                    is_warn = True
                    tmp_message_warn_list.append(value)
            if not is_point_crit and not is_point_warn:
                tmp_message_ok_list.append(value)

    serialized_key = serialize_influx_series_key(key)
    if tmp_message_warn_list:
        tmp_message_warn_dict[serialized_key] = tmp_message_warn_list
    if tmp_message_crit_list:
        tmp_message_crit_dict[serialized_key] = tmp_message_crit_list
    if tmp_message_ok_list:
        tmp_message_ok_dict[serialized_key] = tmp_message_ok_list


# -------------------------------------------------------------------------
# EDGE CASE: NO DATA

if is_resultset_empty:
    nagios_status = 3
    nagios_status_desc = 'UNKNOWN'
    message = 'No data returned from query'
    message = nagios_status_desc + " - " + message
    print(message)
    exit(nagios_status)


# -------------------------------------------------------------------------
# PERF DATA CONSTRUCTION

perfData = ""

perfDataDict = {}
for series_name, tmp_message_ok_list in tmp_message_ok_dict.items():
    farthest = get_farthest_point_from_thresholds(tmp_message_ok_list, args.critical)
    if series_name == 'None':
        perfDataKey = 'value'
    else:
        perfDataKey = series_name
    perfDataDict[perfDataKey] = farthest
for series_name, tmp_message_warn_list in tmp_message_warn_dict.items():
    farthest = get_farthest_point_from_thresholds(tmp_message_warn_list, args.critical)
    if series_name == 'None':
        perfDataKey = 'value'
    else:
        perfDataKey = series_name
    perfDataDict[perfDataKey] = farthest
for series_name, tmp_message_crit_list in tmp_message_crit_dict.items():
    farthest = get_farthest_point_from_thresholds(tmp_message_crit_list, args.critical)
    if series_name == 'None':
        perfDataKey = 'value'
    else:
        perfDataKey = series_name
    perfDataDict[perfDataKey] = farthest

perfDataList = []
for k, v in perfDataDict.items():
    perfDataList.append("'" + k + "'=" + str(v))
perfData = ';'.join(perfDataList)


# -------------------------------------------------------------------------
# OUTPUT CONSTRUCTION

nagios_status = 0
nagios_status_desc = 'OK'
message = "Everything OK"

if is_crit:
    tmp_message_list = []
    for series_name, tmp_message_crit_list in tmp_message_crit_dict.items():
        tmp_message_crit_list2 = [str(i) for i in tmp_message_crit_list]
        tmp_message = ', '.join(tmp_message_crit_list2)
        if series_name != 'None':
            tmp_message = series_name + ' (' + tmp_message + ')'
        tmp_message_list.append(tmp_message)
    message = args.output_template % ', '.join(tmp_message_list)

    nagios_status = 2
    nagios_status_desc = 'CRITICAL'
elif is_warn:
    tmp_message_list = []
    for series_name, tmp_message_warn_list in tmp_message_warn_dict.items():
        tmp_message_warn_list2 = [str(i) for i in tmp_message_warn_list]
        tmp_message = ', '.join(tmp_message_warn_list2)
        if series_name != 'None':
            tmp_message = series_name + ' (' + tmp_message + ')'
        tmp_message_list.append(tmp_message)
    message = args.output_template % ', '.join(tmp_message_list)

    nagios_status = 1
    nagios_status_desc = 'WARNING'


# -------------------------------------------------------------------------
# ANSWER

message = nagios_status_desc + " - " + message
if perfData:
    message += "|" + perfData
print(message)
exit(nagios_status)
