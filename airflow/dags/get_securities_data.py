from airflow import DAG 
from datetime import datetime
import pendulum 
from airflow.models import Variable
from lib.eod_data_downloader import download_massive_eod_data_to_csv
import logging
from airflow.operators.python import PythonOperator
import os
from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator


log = logging.getLogger(__name__)


DEFAULT_ARGS = {
    'owner': 'DataEng',
    'retries': 3,
    'retry_delay': pendulum.duration(minutes=5)
}


MASSIVE_API_KEY = Variable.get('MASSIVE_API_KEY')
MASSIVE_MAX_LOOKBACK_DAYS = int(Variable.get('LOOKBACK_DAYS', "10"))
S3_BUCKET = Variable.get("S3_BUCKET")

with DAG(
    'massive_eod_securities_data_downloader_v1_final',
    start_date = datetime(year=2026, month=4, day=19)  ,
    schedule = '0 12 * * 1-5',
    catchup = False,
    max_active_runs = 1,
    default_args = DEFAULT_ARGS,
    tags = ['massive', 'batch', 'securities'],
    description = 'Massive-Only batch eod: Downloda and process the latest avilable trading date'
): 
    def download_trading_day_csv(**ctx):
        trading_date = download_massive_eod_data_to_csv(MASSIVE_API_KEY, MASSIVE_MAX_LOOKBACK_DAYS)
        ctx['ti'].xcom_push(key='trading_date', value=trading_date)
        log.info(f'Downloding data for date {trading_date}')


    def verify_file_exist(**ctx):
        trading_date = ctx['ti'].xcom_pull(task_ids='t01_download_to_csv', key='trading_date')
        path = f"/tmp/eod_{trading_date}.csv"
        log.info("[verify] Expecting file at: %s", path)

        if not os.path.exists(path):
            raise AirflowException(f'Expected file not found at:{path}')
        
        log.info('[verify] Expecting file found at: %s', path)

        
    download = PythonOperator(
        task_id='t01_download_to_csv',
        python_callable=download_trading_day_csv
    )
    
    verify_file = PythonOperator(
        task_id='t02_verify_local_file',
        python_callable=verify_file_exist
    )

    upload_file = LocalFilesystemToS3Operator(
        task_id="t03_upload_to_s3",
        filename="/tmp/eod_{{ti.xcom_pull(task_ids='t01_download_to_csv', key='trading_date')}}.csv",
        dest_bucket=S3_BUCKET, # S3 bucket where the file will be uploaded
        dest_key=(
            "market/bronze/eod/"
            "eod_prices_{{ ti.xcom_pull(task_ids='t01_download_to_csv', key='trading_date') }}.csv"
        ),
        aws_conn_id="aws_default",  # AWS connection ID to fetch credentials
        replace=True,  # Replace the file if it already exists in S3
    )
    download >> verify_file >> upload_file