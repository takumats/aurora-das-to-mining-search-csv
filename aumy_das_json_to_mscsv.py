import argparse
import json
import csv

# Set true to skip 'rdsadmin' user activity
SKIP_RDSADMIN = True
# Set no. of steps for progress display
LINE_COUNT_STEP = 100000

def write_csv_lines_for_the_file(filename, csv_writer, session_start_time, session_end_time):
    print('Create sql list and output to the file')
    line_count = 0
    with open(filename, encoding='utf8', newline='') as f:
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
                if activityEvent['command'] != 'QUERY':
                    continue

                log_time = activityEvent['logTime']
                a_sql_start_time = log_time[0:4] + log_time[5:7] + log_time[8:10] + log_time[11:13] + log_time[14:16] + log_time[17:19]

                # skip if no session start info
                session_id = activityEvent['sessionId']
                if session_id not in session_start_time:
                    continue

                a_sql_start_time_msec = log_time[20:26]

                row_to_be_written = []
                row_to_be_written.append(activityEvent['serverHost']) # Host
                row_to_be_written.append(activityEvent['databaseName']) # Database
                row_to_be_written.append(session_id) # SID
                row_to_be_written.append('')     # Serial
                row_to_be_written.append(session_start_time[session_id])     # Logged In
                row_to_be_written.append(session_end_time[session_id] if session_id in session_end_time else '')     # Logged Out
                row_to_be_written.append(activityEvent['dbUserName']) # DB User
                row_to_be_written.append(a_sql_start_time) # SQL Start Time
                row_to_be_written.append(a_sql_start_time_msec) # SQL Start Time(Micro Sec)
                row_to_be_written.append(activityEvent['commandText']) # SQL Text
                row_to_be_written.append('')     # Bind Variables
                row_to_be_written.append('')     # Object
                row_to_be_written.append('')     # Elapsed Time
                row_to_be_written.append('')     # Program
                row_to_be_written.append(activityEvent['remoteHost']) # Client Information - Host

                csv_writer.writerow(row_to_be_written)

# Check session information for the specified file
def create_session_list(filename):
    session_start_time = {}
    session_end_time = {}

    # Check by CONNECT/DISCONNECT
    print('Create session list by CONNECT/DISCONNECT')
    line_count = 0
    with open(filename, encoding='utf8', newline='') as f:
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

                session_id = activityEvent['sessionId']
                log_time = activityEvent['logTime']
                if activityEvent['command'] == 'CONNECT':
                    a_session_start_time = log_time[0:4] + log_time[5:7] + log_time[8:10] + log_time[11:13] + log_time[14:16] + log_time[17:19]
                    #print(log_time)
                    #print(a_session_start_time)
                    if session_id in session_start_time:
                        print('ERROR: same session id ' + session_id + ', ' + session_start_time[session_id] + ', ' + a_session_start_time)
                        break
                    session_start_time[session_id] = a_session_start_time
                elif activityEvent['command'] == 'DISCONNECT':
                    a_session_end_time = log_time[0:4] + log_time[5:7] + log_time[8:10] + log_time[11:13] + log_time[14:16] + log_time[17:19]
                    if session_id in session_end_time:
                        print('ERROR: same session id ' + session_id + ', ' + session_end_time[session_id] + ', ' + a_session_end_time)
                        break
                    session_end_time[session_id] = a_session_end_time
    print('  processed:' + str(line_count))
    
    # Check by QUERY (if no CONNECT info, use first query as login and last query as logout)
    print('Create session list by QUERY')
    line_count = 0
    with open(filename, encoding='utf8', newline='') as f:
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
                if activityEvent['command'] != 'QUERY':
                    continue
                session_id = activityEvent['sessionId']

                log_time = activityEvent['logTime']
                a_sql_start_time = log_time[0:4] + log_time[5:7] + log_time[8:10] + log_time[11:13] + log_time[14:16] + log_time[17:19]

                # skip if no session start info
                if session_id not in session_start_time:
                    # use this sql start time
                    session_start_time[session_id] = a_sql_start_time
                if session_id not in session_end_time:
                    # use this sql start time
                    session_end_time[session_id] = a_sql_start_time
                
                if a_sql_start_time < session_start_time[session_id]:
                    session_start_time[session_id] = a_sql_start_time
                if session_end_time[session_id] < a_sql_start_time:
                    session_end_time[session_id] = a_sql_start_time
        print('  processed:' + str(line_count))

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
        writer = csv.writer(fo, quotechar='"', quoting=csv.QUOTE_NONNUMERIC)
        CSV_HEADER = ['Host','Database','SID','Serial','Logged In','Logged Out','DB User','SQL Start Time','SQL Start Time(Micro Sec)','SQL Text','Bind Variables','Object','Elapsed Time','Program','Client Information - Host']

        writer.writerow(CSV_HEADER)
        write_csv_lines_for_the_file(aumy_das_json, writer, session_start_time, session_end_time)

if __name__ == '__main__':
    main()