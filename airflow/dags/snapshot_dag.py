from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# Default arguments for production stability
default_args = {
    'owner': 'shees_ali',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Change these paths to your actual system paths
# Yeh paths ab Docker (Linux) ke mutabiq hain
DBT_BIN = "/home/airflow/.local/bin/dbt" 
DBT_PROJECT_DIR = "/opt/airflow/dags/banking_dbt"
DBT_PROFILES_DIR = "/opt/airflow/dags/banking_dbt"

with DAG(
    dag_id='banking_data_pipeline_v1',
    default_args=default_args,
    description='Full Banking ETL: Snapshot -> Run -> Test',
    schedule_interval=timedelta(days=1), # Run every 24 hours later
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['dbt', 'snowflake', 'banking']
) as dag:

    # 1. Capture History (Snapshots)
    task_snapshot = BashOperator(
        task_id='dbt_snapshot',
        bash_command=f'{DBT_BIN} snapshot --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROFILES_DIR}'
    )

    # 2. Transform Data (Models)
    task_run = BashOperator(
        task_id='dbt_run',
        bash_command=f'{DBT_BIN} run --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROFILES_DIR}'
    )

    # 3. Quality Check (Tests)
    task_test = BashOperator(
        task_id='dbt_test',
        bash_command=f'{DBT_BIN} test --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROFILES_DIR}'
    )

    # Setting up Dependencies
    task_snapshot >> task_run >> task_test