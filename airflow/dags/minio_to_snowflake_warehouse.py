from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from snowflake.connector.pandas_tools import write_pandas
from datetime import datetime, timedelta
import boto3
import pandas as pd
from io import BytesIO
import logging

# 1. Logging setup 
logger = logging.getLogger(__name__)

# 2. DAG setting
default_args = {
    'owner': 'shees_ali_akbar',
    'retries': 2,                 
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 1, 1),
}
# 3. Main Logic  move data  Minio to  Snowflake
def move_data_to_snowflake(table_name, **kwargs):
    # --- STEP A: MinIO se Connect karna ---
    s3 = boto3.client(
        's3',
        endpoint_url='http://minio:9000', 
        aws_access_key_id='minioadmin',
        aws_secret_access_key='minioadmin'
    )
    
    bucket = 'rawbanking'
    
    # --- STEP B: Files listing
    response = s3.list_objects_v2(Bucket=bucket, Prefix=f"{table_name}/")
    
    if 'Contents' not in response:
        logger.info(f"⚠️ {table_name} ke liye koi naya data nahi mila.")
        return f"No data for {table_name}"

    # --- STEP C: Read the complete Parquet files at once ---
    dfs = []
    for obj in response['Contents']:
        if obj['Key'].endswith('.parquet'):
            file_obj = s3.get_object(Bucket=bucket, Key=obj['Key'])
            df = pd.read_parquet(BytesIO(file_obj['Body'].read()))
            dfs.append(df)
    
    if not dfs:
        return "No parquet files found."

    # Collect the complete data
    final_df = pd.concat(dfs, ignore_index=True)
    
    # Snowflake column name neccessary in upper case
    final_df.columns = [col.upper() for col in final_df.columns]


    sf_hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
    conn = sf_hook.get_conn()
    
    try:
        # Snowflake  data "Push" 
        success, nchunks, nrows, _ = write_pandas(
            conn=conn,
            df=final_df,
            table_name=table_name.upper(),
            database='BANKING',
            schema='RAW_BANKING'
        )
        
        if success:
            logger.info(f"✅ SUCCESS: {nrows} rows loaded into {table_name}")
            return f"Loaded {nrows} rows"
            
    finally:
        conn.close()

# 4. DAG Structure (Workflow)
with DAG(
    'banking_ingestion_snowflake_v1',
    default_args=default_args,
    schedule_interval='@daily', 
    catchup=False,
    description='Master DAG to sync MinIO with Snowflake'
) as dag:

    
    tables = ['customers', 'accounts', 'transactions', 'loans', 'branches', 'employees']

    # LOOP
    for t_name in tables:
        ingest_task = PythonOperator(
            task_id=f'ingest_{t_name}',
            python_callable=move_data_to_snowflake,
            op_kwargs={'table_name': t_name} # Table ka naam function ko bhejna
        )

        ingest_task # Tasks ko define kar dena