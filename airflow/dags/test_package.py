import snowflake.connector
from lib.snowcommon import snow_config
from lib.mssql2017 import mssqlconfig
import pymssql
import logging

def snow_test():
    # Gets the version
    # snow acount is the url portion befor snowflakecomputing.com
    snowacct ='db74713.east-us-2.azure'
    ctx = snowflake.connector.connect(**snow_config)
    cs = ctx.cursor()
    try:
        cs.execute("SELECT current_version()")
        one_row = cs.fetchone()
        logging.info(f"Connected to snowflake {snowacct} server , verison = {one_row[0]}")
    finally:
        cs.close()
    ctx.close()



def mssql_test():
#    SERVERNAME = '172.18.0.8'
#    USERNAME = 'sa'
#    PASSWORD = 'sapwd02872$'
#    DATABASE = 'learn'
    output_file_dict ={}
    with  pymssql.connect(server=mssqlconfig['servername'], user=mssqlconfig['username'],
                                password=mssqlconfig['password'], database=mssqlconfig['database']) as dbconn:
    # IF YOU USE NATIVE SQL SERVER INSTALL, YOU CAN REMOVE USERNAME AND PASSWORD FROM INPUT
    # IT SHOULD DEFAULT TO INTEGRATED (WINDOWS) AUTHENTICATION  -- TO BE VERIFIED
        with dbconn.cursor() as cursor: 
            cursor.execute('SELECT @@version')
            row = cursor.fetchone()
            logging.info(f"Connected to SQL server{mssqlconfig['servername']}.{mssqlconfig['database']} , verison = {row[0]}")



"""Example DAG demonstrating the usage of the PythonOperator."""
import time

from airflow import DAG
from airflow.operators.python import PythonOperator, PythonVirtualenvOperator
from airflow.utils.dates import days_ago

args = {
    'owner': 'airflow',
}

with DAG(
    dag_id='test_package_install',
    default_args=args,
    schedule_interval=None,
    start_date=days_ago(2),
    tags=['onecareer','package_validation'],
) as dag:

    # [START howto_operator_python]
    mssqltest= PythonOperator(
        task_id='mssqltest',
        python_callable=mssql_test,
    )
    # [END howto_operator_python]

    snowflaketest = PythonOperator(
        task_id='snowflaketest',
        python_callable = snow_test,
    )
   # [END howto_operator_python_venv]
