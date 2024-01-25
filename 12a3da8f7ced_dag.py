from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator, DatabricksRunNowOperator
from datetime import datetime, timedelta 


# Define params for Submit Run Operator
notebook_task = {
    'notebook_path': '/Users/gracewrightmain@gmail.com/pinterest_batch_data_and_cleaning',
}


# Define params for Run Now Operator
notebook_params = {
    "Variable":5
}


default_args = {
    'owner': '12a3da8f7ced',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2)
}


with DAG('12a3da8f7ced_dag',
    # Enter desired start date
    start_date=datetime(2024, 1, 21),
    # Check out possible intervals, should be a string
    schedule_interval='@daily',
    catchup=False,
    default_args=default_args
    ) as dag:


    opr_submit_run = DatabricksSubmitRunOperator(
        task_id='submit_run',
        # Previously set up connection
        databricks_conn_id='databricks_default',
        existing_cluster_id='1108-162752-8okw8dgg',
        notebook_task=notebook_task
    )
    opr_submit_run
