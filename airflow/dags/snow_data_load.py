from datetime import timedelta
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
import logging


def getCopySql() -> str:
    sql = """
    COPY INTO  LEARN.STG_ORDER(
    ORDERDATE, SALESORDERID, PRODUCTID,POSTALCODE,
    PRODUCTNAME,PRODUCTORDERQTY, PRODUCTSALES,
    SYS_CHANGE_OP,ORDERUPDATEDTM,
    SRC_CDT_BATCH_ID)
    FROM(
        SELECT 
            cast($1:OrderDate as date) as orderdate, 
            cast($1:salesorderid as bigint) as salesorderid,
            cast($1:ProductID as bigint) as produdctid,
            cast($1:PostalCode as varchar) as postalcode, 
            cast($1:productname as varchar) as productname,
            cast($1:productorderqty as int) as productorderqty,
            cast($1:productsales as double) as productsales,
            cast($1:sys_change_op as varchar) as sys_chnage_op, 
            cast($1:orderupdatedtm as timestamp) as orderupdatedtm, 
            cast($1:job_exec_id as bigint) as src_cdt_batch_id
        FROM  @LEARN.MY_STAGE/stg_order/ 
            (file_format => 'LEARN.NDJSON' ) t1
    )PURGE = TRUE;
    """
    return sql


def getLoadTgtSql() -> str:

    sql = """
MERGE INTO LEARN.ORDERS A 
USING 
(
        select  salesorderid, orderdate,postalcode,productid,productname,
                orderupdatedtm, src_cdt_batch_id,SYS_CHANGE_OP , 
                sum(productorderqty) as productorderqty, 
                sum(productsales) as productsales
        from LEARN.STG_ORDER 
        group by salesorderid, orderdate,postalcode,productid,productname,
                orderupdatedtm, src_cdt_batch_id,SYS_CHANGE_OP 
)B 
ON 
A.SALESORDERID = B.SALESORDERID 
AND A.PRODUCTID = B.PRODUCTID 
WHEN MATCHED THEN UPDATE SET 
ORDERDATE = CASE WHEN B.SYS_CHANGE_OP <> 'D' THEN B.ORDERDATE ELSE A.ORDERDATE END , 
PRODUCTID = CASE WHEN B.SYS_CHANGE_OP <> 'D' THEN B.PRODUCTID ELSE A.PRODUCTID END, 
POSTALCODE = CASE WHEN B.SYS_CHANGE_OP <> 'D' THEN B.POSTALCODE ELSE A.POSTALCODE END, 
PRODUCTNAME = CASE WHEN B.SYS_CHANGE_OP <> 'D' THEN B.PRODUCTNAME ELSE A.PRODUCTNAME END, 
PRODUCTORDERQTY = CASE WHEN B.SYS_CHANGE_OP <> 'D' THEN B.PRODUCTORDERQTY ELSE A.PRODUCTORDERQTY END, 
PRODUCTSALES = CASE WHEN B.SYS_CHANGE_OP <> 'D' THEN B.PRODUCTSALES ELSE A.PRODUCTSALES END,
ORDERUPDATEDTM = B.ORDERUPDATEDTM,
SRC_CDT_BATCH_ID = B.SRC_CDT_BATCH_ID, 
DELETED_FLG = CASE WHEN B.SYS_CHANGE_OP = 'D' THEN 1 ELSE 0 END
WHEN NOT MATCHED THEN INSERT (
  SALESORDERID ,
  PRODUCTID ,
  ORDERDATE , 
  PRODUCTNAME ,
  POSTALCODE , 
  PRODUCTORDERQTY , 
  PRODUCTSALES , 
  ORDERUPDATEDTM ,
  SRC_CDT_BATCH_ID ,
  DELETED_FLG 
)VALUES (
B.SALESORDERID ,
B.PRODUCTID ,
B.ORDERDATE , 
B.PRODUCTNAME ,
B.POSTALCODE , 
B.PRODUCTORDERQTY , 
B.PRODUCTSALES , 
B.ORDERUPDATEDTM ,
B.SRC_CDT_BATCH_ID ,
0
);
    """

    return sql


default_args = {
    'owner': 'onecareer',
    'depends_on_past': True,
    'email': ['donotemail@onecareer.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='load_data_to_snow',
    default_args=default_args,
    description='load data from stg to snow target',
    schedule_interval=timedelta(seconds=3600),
    start_date=days_ago(1),
    tags=['onecareer', 'finalproject'],
) as dag:
    """
    this dag will take data out of stage and load to snowflake target tables
    """
    copy_stage = SnowflakeOperator(snowflake_conn_id='snow_conn', task_id='copy_to_stg_orders',
                                   sql=getCopySql()
                                   )
    load_target = SnowflakeOperator(snowflake_conn_id="snow_conn", sql=getLoadTgtSql(), task_id='load_to_orders'
                                    )
    purge_stg = SnowflakeOperator(snowflake_conn_id='snow_conn', task_id='purge_stg_orders',
                                  sql='DELETE FROM LEARN.STG_ORDER')
    copy_stage >> load_target >> purge_stg
