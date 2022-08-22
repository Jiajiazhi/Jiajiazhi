from datetime import timedelta
import os
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from util.sql_cdc import job_start, job_end, get_last_success_version, process_changed_data
import logging

default_args = {
    'owner': 'onecareer',
    'depends_on_past': False,
    'email': ['donotemail@onecareer.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


def getSnowConnection():
    return SnowflakeHook(snowflake_conn_id='snow_conn').get_conn()


def getSqlServerConnection():
    return MsSqlHook(mssql_conn_id='mssql2017').get_conn()


def upload_file_to_snowflake(dbconn, filename: str) -> None:
    with dbconn.cursor() as cur:
        sql = f'put file://{filename} @LEARN.MY_STAGE/stg_order/'
        cur.execute(sql)
        logging.info(f'successfully uploaded file {filename}')
#        os.remove(filename)
        logging.info(f'successfully removed file {filename}')


def upload_file_to_snowflake_wrapper(**kwargs) -> None:
    with getSnowConnection() as dbconn:
        filename = kwargs['ti'].xcom_pull(
            key='outputfile', task_ids='process_changed_data')
        logging.info(f'get xcom value outputfile = {filename}')
        upload_file_to_snowflake(dbconn=dbconn, filename=filename)


def job_start_wrapper(**kwargs) -> int:
    with getSqlServerConnection() as dbconn:
        job_exec_id = job_start(dbconn=dbconn)
        kwargs['ti'].xcom_push(key='job_exec_id', value=job_exec_id)
    return job_exec_id


def get_last_success_version_wrapper(**kwargs) -> int:
    with getSqlServerConnection() as dbconn:
        last_sync_ver = get_last_success_version(dbconn=dbconn)
        kwargs['ti'].xcom_push(key='last_sync_ver', value=last_sync_ver)
    return last_sync_ver


def process_changed_data_wrapper(**kwargs) -> str:
    with getSqlServerConnection() as dbconn:
        job_exec_id = kwargs['ti'].xcom_pull(
            key='job_exec_id', task_ids='job_start')
        last_sync_ver = kwargs['ti'].xcom_pull(
            key='last_sync_ver', task_ids='get_last_success_version'
        )
        logging.info(
            f'get xcom value job_exec_id = {job_exec_id}  last_sync_ver = {last_sync_ver}')
        outfilename = process_changed_data(
            dbconn=dbconn, job_exec_id=job_exec_id, last_sync_version_id=last_sync_ver)
        kwargs['ti'].xcom_push(key='outputfile', value=outfilename)
        logging.info('push xcom value key=outputfile, value={outfilename}')
        return outfilename


def job_end_wrapper(**kwargs) -> None:
    with getSqlServerConnection() as dbconn:
        job_exec_id = kwargs['ti'].xcom_pull(
            key='job_exec_id', task_ids='job_start')
        logging.info(f'get xcom value job_exec_id = {job_exec_id}')
        job_end(dbconn=dbconn, job_exec_id=job_exec_id)


with DAG(
    dag_id='capture_change_data',
    default_args=default_args,
    description='capture changes in order tables',
    schedule_interval=timedelta(seconds=1800),
    start_date=days_ago(1),
    tags=['onecareer', 'finalproject'],
) as dag:
    """
    """

    job_start_task = PythonOperator(python_callable=job_start_wrapper,
                                    task_id='job_start'
                                    )
    get_last_success_version_task = PythonOperator(python_callable=get_last_success_version_wrapper,
                                                   task_id='get_last_success_version')
    process_changed_data_task = PythonOperator(python_callable=process_changed_data_wrapper,
                                               task_id='process_changed_data')
    job_end_task = PythonOperator(python_callable=job_end_wrapper,
                                  task_id='job_end')
    upload_file_to_stage_task = PythonOperator(
        python_callable=upload_file_to_snowflake_wrapper, task_id='upload_file_to_stage')
    job_start_task >> get_last_success_version_task >> process_changed_data_task >> upload_file_to_stage_task >> job_end_task
