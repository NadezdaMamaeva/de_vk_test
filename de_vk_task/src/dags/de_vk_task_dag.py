import os

from datetime import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator


### Настройка путей ###

os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['JAVA_HOME']='/usr'
os.environ['SPARK_HOME'] ='/usr/lib/spark'
os.environ['PYTHONPATH'] ='/usr/local/lib/python3.8'


### DAG launch settings ###

# get date from variable
process_date = Variable.get('process_date')
# get path to csv data from variable
path_csv_file = Variable.get('path_csv_file')
# get path to writing results from variable
path_result = Variable.get('path_result')


### Объявление DAG ###

# конфигурация
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 9, 25),  # Дата начала выполнения дага. Можно поставить сегодня.
}

# DAG

dag_spark = DAG(
    dag_id = "de_vk_taks_mamaevano",
    default_args = default_args,
    schedule_interval = '0 7 * * *',  # Задаем расписание выполнения дага - по расписанию каждый день в 7:00.
)


# Задача расчёта недельного агрегата действий пользователей на указанную дату
user_actions = SparkSubmitOperator(
    task_id = 'mart_users',
    dag  =  dag_spark,
    application = '/<указать путь до скрипта расчёта>/de_vk_task_spark.py',
    conn_id= 'yarn_spark',
    application_args = [
        process_date,
        path_csv_file,
        path_result
    ],
    conf={
        "spark.driver.maxResultSize": "1g"
    },
    executor_cores = 1,
    executor_memory = '1g'
)


user_actions
