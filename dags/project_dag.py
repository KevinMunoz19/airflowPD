import os
from datetime import datetime

import pandas as pd
from airflow import DAG
from airflow.contrib.hooks.fs_hook import FSHook
from airflow.hooks.mysql_hook import MySqlHook
from airflow.contrib.sensors.file_sensor import FileSensor
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from structlog import get_logger

logger = get_logger()


dag = DAG('project_dag', description='A DAG for final project',
          default_args={
              'owner': 'obed.espinoza',
              'depends_on_past': False,
              'max_active_runs': 1,
              'start_date': days_ago(5)
          },
          schedule_interval='0 1 * * *',
          catchup=False)


def process_file(**kwargs):
    logger.info(kwargs['execution_date'])
    file_path = f"{FSHook('fs_default').get_path()}/time_series_covid19_confirmed_global.csv"
    file_path_recovered = f"{FSHook('fs_default').get_path()}/time_series_covid19_recovered_global.csv"
    file_path_deaths = f"{FSHook('fs_default').get_path()}/time_series_covid19_deaths_global.csv"

    connection = MySqlHook('mysql_default').get_sqlalchemy_engine()

    df = (pd.read_csv(file_path, encoding="ISO-8859-1"))
    df = pd.melt(df, id_vars=['Province/State', 'Country/Region', 'Lat', 'Long'], var_name='date_d', value_name="value")
    df = df.rename(
        columns={'Province/State': 'state', 'Country/Region': 'country', 'Lat': 'lat', 'Long': 'lon'}, inplace=False)

    df_recovered = (pd.read_csv(file_path_recovered, encoding="ISO-8859-1"))
    df_recovered = pd.melt(df_recovered, id_vars=['Province/State', 'Country/Region', 'Lat', 'Long'], var_name='date_d', value_name="value")
    df_recovered = df_recovered.rename(
        columns={'Province/State': 'state', 'Country/Region': 'country', 'Lat': 'lat', 'Long': 'lon'}, inplace=False)

    df_deaths = (pd.read_csv(file_path_deaths, encoding="ISO-8859-1"))
    df_deaths = pd.melt(df_deaths, id_vars=['Province/State', 'Country/Region', 'Lat', 'Long'], var_name='date_d', value_name="value")
    df_deaths = df_deaths.rename(
        columns={'Province/State': 'state', 'Country/Region': 'country', 'Lat': 'lat', 'Long': 'lon'}, inplace=False)

    logger.info(df)

    with connection.begin() as transaction:
        transaction.execute('Delete from test.conf where 1=1')
        transaction.execute('Delete from test.recovered where 1=1')
        transaction.execute('Delete from test.deaths where 1=1')
        df.to_sql('conf', con=transaction, schema='test', if_exists='append', index=False)
        df_recovered.to_sql('recovered', con=transaction, schema='test', if_exists='append', index=False)
        df_deaths.to_sql('deaths', con=transaction, schema='test', if_exists='append', index=False)

    logger.info(f'Records inserted {len(df.index)}')

sensor = FileSensor(filepath='time_series_covid19_confirmed_global.csv',
                    fs_conn_id='fs_default',
                    task_id='check_for_file',
                    poke_interval=5,
                    timeout=60,
                    dag=dag
                    )

operador = PythonOperator(task_id='process_file',
                          dag=dag,
                          python_callable=process_file,
                          provide_context=True
)

sensor >> operador
