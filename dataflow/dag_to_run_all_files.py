from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

# Define default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
with DAG(
    'run_python_scripts',
    default_args=default_args,
    description='A simple DAG to run Python scripts one by one',
    schedule='@daily',  # Run manually or set a cron expression if needed
    start_date=datetime(2024, 11, 13),
    catchup=False,
) as dag:

    # Task to run the first Python script
    run_script1 = BashOperator(
        task_id='historical_data',
        bash_command=r'gs://earthquick_project/dataflow/final_project_all_data/daily.py',
    )



    # Define task dependencies
    run_script1
