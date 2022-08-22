$containers = ('airflow_flower_1','airflow_airflow-worker_1','airflow_airflow-webserver_1','airflow_airflow-scheduler_1')
foreach ($c in $containers)
{
    docker cp .\airflow.cfg "${c}:/opt/airflow"
    docker exec -it -u root ${c} apt-get update
    docker exec -it -u root ${c} apt-get install -y libssl-dev libffi-dev
    docker exec -it ${c} pip install -r https://raw.githubusercontent.com/snowflakedb/snowflake-connector-python/v2.5.1/tested_requirements/requirements_36.reqs
    docker exec -it ${c} pip install snowflake-connector-python==2.5.1
    docker exec -it ${c} pip install apache-airflow-providers-snowflake
    docker exec -it ${c} pip install apache-airflow-providers-microsoft-mssql
    docker exec -it ${c} pip install ndjson 
    docker restart ${c}
}
