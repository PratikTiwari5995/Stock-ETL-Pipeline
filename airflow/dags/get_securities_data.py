from airflow import DAG
import pendulum
from airflow.models import Variable  
from airflow.exceptions import (
    AirflowFailException,
)  
from airflow.providers.standard.operators.python import (
    PythonOperator,
)  
import datetime
import os
from airflow.sdk import TaskGroup
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.amazon.aws.transfers.local_to_s3 import (
    LocalFilesystemToS3Operator,
)
from airflow.providers.snowflake.operators.snowflake import SnowflakeCheckOperator


from lib.eod_data_downloader import (
    download_massive_eod_data_to_csv,
)  

import logging


log = logging.getLogger(__name__)


DEFAULT_ARGS = {
    "owner": "DataEng",
    "retries": 3,
    "retry_delay": pendulum.duration(minutes=5),
}


MASSIVE_API_KEY = Variable.get("MASSIVE_API_KEY")
MASSIVE_MAX_LOOKBACK_DAYS = int(Variable.get("LOOKBACK_DAYS", "10"))
S3_BUCKET = Variable.get("S3_BUCKET")
TEMPLATE_SEARCHPATH = [os.path.join(os.path.dirname(__file__), "sql")]

with DAG(
    "massive_eod_securities_data_downloader_v1_final",
    start_date=datetime.datetime(year=2026, month=4, day=19),
    schedule="0 12 * * 1-5",
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["massive", "batch", "securities"],
    description="Massive-Only batch eod: Downloda and process the latest avilable trading date",
    template_searchpath=TEMPLATE_SEARCHPATH,
):

    def download_trading_day_csv(**ctx):
        trading_date = download_massive_eod_data_to_csv(
            MASSIVE_API_KEY, MASSIVE_MAX_LOOKBACK_DAYS
        )
        ctx["ti"].xcom_push(key="trading_date", value=trading_date)
        log.info(f"Downloding data for date {trading_date}")

    def verify_file_exist(**ctx):
        trading_date = ctx["ti"].xcom_pull(
            task_ids="t01_download_to_csv", key="trading_date"
        )
        path = f"/tmp/eod_{trading_date}.csv"
        log.info("[verify] Expecting file at: %s", path)

        if not os.path.exists(path):
            raise AirflowException(f"Expected file not found at:{path}")

        log.info("[verify] Expecting file found at: %s", path)

    with TaskGroup(group_id="t04_snowflake_load") as snowflake_load:
        params_common = {"trading_ds_task_id": "t01_download_to_csv"}
        copy_to_raw = SQLExecuteQueryOperator(
            task_id="s01_copy_to_raw",
            conn_id="snowflake_default",
            sql="copy_to_raw.sql",
            params=params_common,
        )

        check_loaded = SnowflakeCheckOperator(
            task_id="s02_check_eod_prices_exist",
            sql="check_loaded.sql",
            snowflake_conn_id="snowflake_default",
            params=params_common,
        )

        premerge_metrics = SQLExecuteQueryOperator(
            task_id="s03_compute_premerge_metrics",
            conn_id="snowflake_default",
            sql="premerge_metrics.sql",
            params=params_common,
        )

        merge_core = SQLExecuteQueryOperator(
            task_id="s04_merge_core_eod",
            conn_id="snowflake_default",
            sql="merge_core.sql",
            params=params_common,
        )

        merge_dim_security = SQLExecuteQueryOperator(
            task_id="s05_merge_dim_security",
            conn_id="snowflake_default",
            sql="merge_dim_security.sql",
            params=params_common,
        )

        merge_dim_date = SQLExecuteQueryOperator(
            task_id="s06_merge_dim_date",
            conn_id="snowflake_default",
            sql="dm_dim_date.sql",
            params=params_common,
        )

        merge_fact = SQLExecuteQueryOperator(
            task_id="s07_merge_fact_daily_price",
            conn_id="snowflake_default",
            sql="merge_fact_daily_price.sql",
            params=params_common,
        )

        postmerge = SQLExecuteQueryOperator(
            task_id="s08_compute_postmerge_metrics",
            conn_id="snowflake_default",
            sql="postmerge_metrics.sql",
            params=params_common,
        )


        copy_to_raw >> check_loaded >> premerge_metrics >> merge_core
        merge_core >> [merge_dim_security, merge_dim_date] >> merge_fact >> postmerge

    download = PythonOperator(
        task_id="t01_download_to_csv", python_callable=download_trading_day_csv
    )

    verify_file = PythonOperator(
        task_id="t02_verify_local_file", python_callable=verify_file_exist
    )

    upload_file = LocalFilesystemToS3Operator(
        task_id="t03_upload_to_s3",
        filename="/tmp/eod_{{ti.xcom_pull(task_ids='t01_download_to_csv', key='trading_date')}}.csv",
        dest_bucket=S3_BUCKET,  # S3 bucket where the file will be uploaded
        dest_key=(
            "market/bronze/eod/"
            "eod_prices_{{ ti.xcom_pull(task_ids='t01_download_to_csv', key='trading_date') }}.csv"
        ),
        aws_conn_id="aws_default",  # AWS connection ID to fetch credentials
        replace=True,  # Replace the file if it already exists in S3
    )
    download >> verify_file >> upload_file >> snowflake_load
