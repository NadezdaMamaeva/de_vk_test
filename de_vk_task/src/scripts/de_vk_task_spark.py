import csv
import datetime
import os
import sys


DEFAULT_PATH_CSV_FILE = os.path.join('data', 'de_vk_task.csv')
DEFAULT_PATH_RESULT = os.path.join('output')


def read_csv(path_to_csv):
    result = list()
    with open(path_to_csv, encoding='utf-8') as csv_file:
        file_reader = csv.DictReader(csv_file, delimiter=",")
        for row in file_reader:
            result.append(row)
    return result


def process(data, start_date, end_date):
    result = dict()
    start_dt = start_date.strftime('%Y-%m-%d')
    end_dt = end_date.strftime('%Y-%m-%d')
    for row in data:
        if start_dt <= row['dt'] <= end_dt:
            if not row['email'] in result:
                result[row['email']] = {'CREATE': 0, 'READ': 0, 'UPDATE': 0, 'DELETE': 0}
            result[row['email']][row['action']] += 1
    return result


def write_csv(data, output_path):
    with open(output_path, mode='w', encoding='utf-8') as out_file:
        file_writer = csv.writer(out_file, delimiter=',', lineterminator='\r')
        file_writer.writerow(['email', 'create_count', 'read_count', 'update_count', 'delete_count'])
        for key, value in data.items():
            file_writer.writerow([key, value['CREATE'], value['READ'], value['UPDATE'],
                                  value['DELETE']])


def main():
    print('**********\nIncoming args:')
    print('\n'.join(sys.argv))
    process_date = sys.argv[1]
    if len(sys.argv) >= 3:
        path_csv_file = sys.argv[2]
    else:
        path_csv_file = DEFAULT_PATH_CSV_FILE
    if len(sys.argv) >= 4:
        path_result = sys.argv[3]
    else:
        path_result = DEFAULT_PATH_RESULT
    print('->')
    print(f'process_date = {process_date}')
    print(f'path_csv_file = {path_csv_file}')
    print(f'path_result = {path_result}')
    print('**********')

    csv_data = read_csv(path_csv_file)
    print(csv_data)

    dt = datetime.datetime.strptime(process_date, '%Y-%m-%d')
    start_date = dt-datetime.timedelta(days=7)
    end_date = dt-datetime.timedelta(days=1)
    print(f'\nprocess from "{start_date}" to "{end_date}"')

    processed_data = process(csv_data, start_date, end_date)
    print(processed_data)

    path_out_file = os.path.join(path_result, f'{process_date}.csv')
    write_csv(processed_data, path_out_file)

    return 200


if __name__ == '__main__':
        main()
