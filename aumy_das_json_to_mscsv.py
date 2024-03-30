import argparse
import json
import csv
from dataclasses import dataclass

# Set true to skip 'rdsadmin' user activity
SKIP_RDSADMIN = True
# Set no. of steps for progress display
LINE_COUNT_STEP = 100000

@dataclass
class AuditItem:
    db_host: str = ''
    db_name: str = ''
    db_user: str = ''
    session_id: str = ''
    timestamp: str = ''
    timestamp_msec: str = ''
    sql_bind: str = ''
    sql_text: str = ''
    remote_host: str = ''
    command: str = ''

def ParseAumyDasJsonFile(filename):
    with open(filename, encoding='utf8', newline='') as f:
        line_count = 0
        for line in f:
            line_count = line_count + 1
            if (line_count % LINE_COUNT_STEP == 0):
                print('  processed:' + str(line_count))

            json_row = json.loads(line)

            for activityEvent in json_row['databaseActivityEventList']:
                if activityEvent['type'] != 'record':
                    continue
                # skip if SKIP_RDSADMIN is True and the user is 'rdsadmin'
                if SKIP_RDSADMIN and activityEvent['dbUserName'] == 'rdsadmin':
                    continue

                audit_item = AuditItem()
                audit_item.db_host = activityEvent['serverHost']
                audit_item.db_name = activityEvent['databaseName']
                audit_item.db_user = activityEvent['dbUserName']
                audit_item.session_id = activityEvent['sessionId']
                log_time = activityEvent['logTime']
                audit_item.timestamp = log_time[0:4] + log_time[5:7] + log_time[8:10] + log_time[11:13] + log_time[14:16] + log_time[17:19]
                audit_item.timestamp_msec = log_time[20:26]
                audit_item.sql_text = activityEvent['commandText']
                audit_item.remote_host = activityEvent['remoteHost']
                audit_item.command = activityEvent['command']

                yield audit_item

        print('  processed:' + str(line_count))

def write_ms_csv_row(csv_writer, audit_item, session_start_time, session_end_time):
    # skip if no session start info
    if audit_item.session_id not in session_start_time:
        return
    
    row_to_be_written = []
    row_to_be_written.append(audit_item.db_host) # Host
    row_to_be_written.append(audit_item.db_name) # Database
    row_to_be_written.append(audit_item.session_id) # SID
    row_to_be_written.append('')     # Serial
    row_to_be_written.append(session_start_time[audit_item.session_id])     # Logged In
    row_to_be_written.append(session_end_time[audit_item.session_id] if audit_item.session_id in session_end_time else '')     # Logged Out
    row_to_be_written.append(audit_item.db_user) # DB User
    row_to_be_written.append(audit_item.timestamp) # SQL Start Time
    row_to_be_written.append(audit_item.timestamp_msec) # SQL Start Time(Micro Sec)
    row_to_be_written.append(audit_item.sql_text) # SQL Text
    row_to_be_written.append(audit_item.sql_bind) # Bind Variables
    row_to_be_written.append('')     # Object
    row_to_be_written.append('')     # Elapsed Time
    row_to_be_written.append('')     # Program
    row_to_be_written.append(audit_item.remote_host) # Client Information - Host

    csv_writer.writerow(row_to_be_written)

# Check session information for the specified file
def create_session_list(filename):
    session_start_time = {}
    session_end_time = {}

    # Check by CONNECT/DISCONNECT
    print('Create session list by CONNECT/DISCONNECT')
    for audit_item in ParseAumyDasJsonFile(filename):
        if audit_item.command == 'CONNECT':
            if audit_item.session_id in session_start_time:
                print('ERROR: same session id ' + audit_item.session_id + ', ' + session_start_time[audit_item.session_id] + ', ' + audit_item.timestamp)
                break
            session_start_time[audit_item.session_id] = audit_item.timestamp
        elif audit_item.command == 'DISCONNECT':
            if audit_item.session_id in session_end_time:
                print('ERROR: same session id ' + session_id + ', ' + session_end_time[audit_item.session_id] + ', ' + audit_item.timestamp)
                break
            session_end_time[audit_item.session_id] = audit_item.timestamp

    # Check by QUERY (if no CONNECT info, use first query as login and last query as logout)
    print('Create session list by QUERY')
    for audit_item in ParseAumyDasJsonFile(filename):
        if audit_item.command == 'QUERY':
            if audit_item.session_id not in session_start_time:
                # use this sql start time
                session_start_time[audit_item.session_id] = audit_item.timestamp
            if audit_item.session_id not in session_end_time:
                # use this sql start time
                session_end_time[audit_item.session_id] = audit_item.timestamp
            
            if audit_item.timestamp < session_start_time[audit_item.session_id]:
                session_start_time[audit_item.session_id] = audit_item.timestamp
            if session_end_time[audit_item.session_id] < audit_item.timestamp:
                session_end_time[audit_item.session_id] = audit_item.timestamp

    return session_start_time, session_end_time

def main():
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument(
        'aumy_das_json',
        metavar='AUMY_DAS_JSON',
        help='Aurora MySQL DASから出力された内容をJSONにしたファイル'
    )
    arg_parser.add_argument(
        'output_csv_file_name',
        metavar='OUTPUT_CSV_FILE_NAME',
        help='マイニングサーチ形式CSV出力ファイル名'
    )

    # 引数取得
    args = arg_parser.parse_args()
    aumy_das_json = args.aumy_das_json
    output_csv_file_name = args.output_csv_file_name

    # Session Login/Logout information
    session_start_time, session_end_time = create_session_list(aumy_das_json)
    print('no. of sessions:' + str(len(session_start_time)))

    # Create csv file
    with open(output_csv_file_name, 'w') as fo:
        csv_writer = csv.writer(fo, quotechar='"', quoting=csv.QUOTE_NONNUMERIC)
        CSV_HEADER = ['Host','Database','SID','Serial','Logged In','Logged Out','DB User','SQL Start Time','SQL Start Time(Micro Sec)','SQL Text','Bind Variables','Object','Elapsed Time','Program','Client Information - Host']

        csv_writer.writerow(CSV_HEADER)
        for audit_item in ParseAumyDasJsonFile(aumy_das_json):
            if audit_item.command == 'QUERY':
                write_ms_csv_row(csv_writer, audit_item, session_start_time, session_end_time)

if __name__ == '__main__':
    main()