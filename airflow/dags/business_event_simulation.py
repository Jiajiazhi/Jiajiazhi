from datetime import timedelta
from airflow import DAG
from airflow.utils.dates import days_ago

from airflow.providers.microsoft.mssql.operators.mssql import MsSqlOperator
from util.business_events import createOrders, createUsers, createCustomers, createAddresses

default_args = {
    'owner': 'onecareer',
    'depends_on_past': False,
    'email': ['donotemail@onecareer.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    # 'wait_for_downstream': False,
    # 'dag': dag,
    # 'sla': timedelta(hours=2),
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': some_function,
    # 'on_success_callback': some_other_function,
    # 'on_retry_callback': another_function,
    # 'sla_miss_callback': yet_another_function,
    # 'trigger_rule': 'all_success'
}
with DAG(
    dag_id='business_event_simulation',
    default_args=default_args,
    description='Simulate business event in mssql database',
    schedule_interval=timedelta(seconds=600),
    start_date=days_ago(1),
    tags=['onecareer','finalproject'],
) as dag:
    """
    """
    create_users = MsSqlOperator(
        task_id='creat_users',
        # defined connection in airflow admin tool
        mssql_conn_id='mssql2017',
        sql=createUsers()
    )
    create_customers = MsSqlOperator(
        task_id='create_customers',
        mssql_conn_id='mssql2017',
        sql=createCustomers()
    )
    create_addresses = MsSqlOperator(
        task_id='create_addresses',
        mssql_conn_id='mssql2017',
        sql=createAddresses()
    )
    create_orders = MsSqlOperator(
        task_id='create_orders',
        mssql_conn_id='mssql2017',
        sql=createOrders()
    )

    create_users >> create_customers 
    [create_customers,create_addresses]>> create_orders
