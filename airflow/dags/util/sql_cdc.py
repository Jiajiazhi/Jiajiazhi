import pymssql
import sys
from pymssql import Connection
import logging
import ndjson
import tempfile
import logging


def process_cdt(dbconn: Connection = None) -> str:
    """
    process the full cdt cycle, return the output file 
    name 
    """
    if dbconn == None:
        logging.info('creating connection')
        dbconn = createConnection()
    job_exec_id = job_start(dbconn)
    logging.info(f'job_exec_id={job_exec_id}')
    last_ver = get_last_success_version(dbconn)
    logging.info(f'last_sync_ver={last_ver}')
    outputfilename = process_changed_data(
        dbconn=dbconn, job_exec_id=job_exec_id, last_sync_version_id=last_ver)
    logging.info(f'outputfilename={outputfilename}')
    job_end(dbconn=dbconn,job_exec_id=job_exec_id)
    return outputfilename


def createConnection() -> Connection:
    import sys 
    sys.path.append('/opt/airflow/dags')
    from lib.mssql2017 import mssqlconfig
    return pymssql.connect(server=mssqlconfig['servername'], user=mssqlconfig['username'],
                           password=mssqlconfig['password'], database='adventureworks2017')


def get_last_success_version(dbconn: Connection) -> int:
    """
    Last success version from the log table 
    """
    sql = '''
        SELECT TOP 1 DB_CHG_VERSION_ID 
        FROM (
        SELECT  DB_CHG_VERSION_ID, JOB_END_TS
        FROM DBO.CHG_LOG_TBL 
        WHERE JOB_EXEC_STATUS = 0 
        UNION ALL 
        SELECT 0 AS INIT_VERSION, CAST('1980-01-01 00:00:00' AS DATETIME) AS INIT_DT
        ) A 
        ORDER BY JOB_END_TS DESC
    '''
    ret = 0
    with dbconn.cursor() as cursor:
        cursor.execute(sql)
        row = cursor.fetchone()
        ret = row[0]
    logging.debug(f'last_success_version = {ret}')
    return ret


def job_start(dbconn: Connection) -> int:
    start_ins_sql = '''
    DECLARE @CURRENT_CHG_VERSION AS BIGINT;
    SET @CURRENT_CHG_VERSION = CHANGE_TRACKING_CURRENT_VERSION();
    INSERT INTO dbo.CHG_LOG_TBL(
    DB_CHG_VERSION_ID) VALUES (@CURRENT_CHG_VERSION);
    '''
    fetch_id_sql = '''SELECT  SCOPE_IDENTITY() ;'''
    job_exc_id = -1
    with dbconn.cursor() as cursor:
        cursor.execute(start_ins_sql)
        dbconn.commit()
        cursor.execute(fetch_id_sql)
        row = cursor.fetchone()
        job_exc_id = row[0]
    logging.debug(f"job_start.job_exec_id = {job_exc_id}")
    return job_exc_id


def process_changed_data(dbconn: Connection, job_exec_id: int, last_sync_version_id: int) -> str:
    sql = """
with chg as (
select	a.salesorderid, 
	a.sys_change_operation as sys_change_op, 
	sh.ShipToAddressID,
	sh.CustomerID,
	sh.orderdate,
	sh.modifieddate as orderupdatedtm
from	changetable(changes sales.salesorderheader, {LAST_SYNC_VERSION_ID}) a 
	left outer join 
	sales.salesorderheader sh 
	on 
	a.salesorderid = sh.SalesOrderID
)
select	chg.salesorderid, 
	chg.sys_change_op, 
	chg.OrderDate,
	chg.orderupdatedtm,
	so.ProductID, 
	p.Name as productname,
	ad.PostalCode,
    so.orderqty as productorderqty,
	so.LineTotal as productsales,
    {JOB_EXEC_ID} as job_exec_id
from	chg 
	left outer join 
	sales.SalesOrderDetail so 
	on 
	chg.salesorderid = so.salesorderid
	left outer join production.Product p 
	on 
	so.productid = p.ProductID
	left outer join 
	person.Address ad
	on
	chg.ShipToAddressID = ad.AddressID

    """.format(
        JOB_EXEC_ID=job_exec_id,
        LAST_SYNC_VERSION_ID=last_sync_version_id
    )
    total_cnt = 0
    by_type_cnt = {}
    allrows = []
    with dbconn.cursor() as cursor:
        cursor.execute(sql)
        row = cursor.fetchone()
        while row:
            row_in_dic = _convert2dic(row, cursor.description)
            allrows.append(row_in_dic)
            sys_chg_op = row_in_dic['sys_change_op']
            by_type_cnt[sys_chg_op] = by_type_cnt.get(sys_chg_op, 0)+1
            total_cnt += 1
            row = cursor.fetchone()

    logging.info(f"total row count = {total_cnt}")
    for sys_chg_op in by_type_cnt:
        logging.info(f'{sys_chg_op} rowcnt = {by_type_cnt[sys_chg_op]}')
    return _writeDataToFile(allrows)


def _writeDataToFile(rows) -> str:
    outputFile = tempfile.NamedTemporaryFile(mode='w',delete=False)
    filename = outputFile.name
    outputFile.write(ndjson.dumps(rows))
    outputFile.close()
    return filename


def _convert2dic(row, description) -> dict:
    """
    convert turple with columnname:colvalue pairs in dictionary
    """
    rowindic = {}
    idx = 0
    for d in description:
        colname = d[0]
        colvalue = str(row[idx])
        rowindic[colname] = colvalue
        idx += 1
    return rowindic


def job_end(dbconn: Connection, job_exec_id: int) -> None:
    sql = """
    UPDATE DBO.CHG_LOG_TBL 
    SET JOB_EXEC_STATUS = 0 , 
    JOB_END_TS = GETDATE()
    WHERE JOB_EXEC_ID = {JOB_EXEC_ID}
    """.format(JOB_EXEC_ID=job_exec_id)
    with dbconn.cursor() as cursor:
        cursor.execute(sql)
        dbconn.commit()

