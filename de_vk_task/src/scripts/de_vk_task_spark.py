import datetime
import os
import sys

import pyspark.sql.functions as F

from pyspark import SparkConf
from pyspark.context import SparkContext
from pyspark.sql import SQLContext


os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'


conf = SparkConf().setAppName('MamaevaNO_VK_DE_TEST').setMaster('local')
sc = SparkContext(conf=conf)
sql = SQLContext(sc)


def read_csv(path_to_csv):
    result_df = sql.read \
        .option('header', True) \
        .option('delimiter', ',') \
        .csv(path_to_csv)
    return result_df


def process(data_df, start_date, end_date):
    start_dt = start_date.strftime('%Y-%m-%d')
    end_dt = end_date.strftime('%Y-%m-%d')
    tmp_df = data_df.where((F.col('dt') >= start_dt) & (F.col('dt') <= end_dt))
    result_df = tmp_df.groupBy('email').agg(
        F.sum(F.when(F.col('action') == 'CREATE', F.lit(1)).otherwise(F.lit(0))).alias('create_count'),
        F.sum(F.when(F.col('action') == 'READ', F.lit(1)).otherwise(F.lit(0))).alias('read_count'),
        F.sum(F.when(F.col('action') == 'UPDATE', F.lit(1)).otherwise(F.lit(0))).alias('update_count'),
        F.sum(F.when(F.col('action') == 'DELETE', F.lit(1)).otherwise(F.lit(0))).alias('delete_count')
    )
    return result_df


def write_csv(data_df, output_path):
    data_df.write.mode('overwrite').option('header', True).option('delimiter', ',').csv(output_path)


def main():
    print('**********\nIncoming args:')
    print('\n'.join(sys.argv))
    process_date = sys.argv[1]
    path_csv_file = sys.argv[2]
    path_result = sys.argv[3]
    print('->')
    print(f'process_date = {process_date}')
    print(f'path_csv_file = {path_csv_file}')
    print(f'path_result = {path_result}')
    print('**********')

    csv_data_df = read_csv(path_csv_file)
    csv_data_df.persist()

    dt = datetime.datetime.strptime(process_date, '%Y-%m-%d')
    start_date = dt-datetime.timedelta(days=7)
    end_date = dt-datetime.timedelta(days=1)
    print(f'\nprocess from "{start_date}" to "{end_date}"')

    processed_data_df = process(csv_data_df, start_date, end_date)

    path_out_file = os.path.join(path_result, f'{process_date}.csv')
    write_csv(processed_data_df, path_out_file)

    return 200


if __name__ == '__main__':
        main()
