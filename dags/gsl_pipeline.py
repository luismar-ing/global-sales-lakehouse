from datetime import datetime, timedelta
from airflow.sdk import dag
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator

MEDALLION_JOB_ID = 81935214705164

@dag(
    dag_id='gsl_pipeline',
    schedule=None,
    start_date=datetime(2026, 11, 1),
    catchup=False,
    tags=['gsl']
)
def gsl_pipeline():
    DatabricksRunNowOperator(
        task_id="run_medallion_job",
        databricks_conn_id="databricks_default",
        job_id=MEDALLION_JOB_ID,
        execution_timeout=timedelta(minutes=30),
        retries=1,
        retry_delay=timedelta(minutes=5)
    )

gsl_pipeline()