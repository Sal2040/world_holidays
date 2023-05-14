from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
import os

HOME_DIR = os.environ.get("WH_HOME")
EXTRACT_SCRIPT = os.path.join(HOME_DIR, 'extract.py')
TRANSFORM_LOAD_SCRIPT = os.path.join(HOME_DIR, 'transform_load.py')

with DAG(
    dag_id="wh_extract_load",
    default_args={
        "depends_on_past": False,
        "email": ["sal2040.dev@gmail.com"],
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 0,
        "retry_delay": timedelta(minutes=1),
    },
    description="world_holidays_extract_load_DAG",
    # extract data for a new year every December 27 at 12:00:
    schedule="0 12 27 12 *",
    start_date=datetime(2023, 4, 1),
    catchup=False,
    tags=["wh"],
    ) as dag:

    t1 = BashOperator(
        task_id="extract",
        bash_command=f"python {EXTRACT_SCRIPT}",
    )

    t2 = BashOperator(
        task_id="load",
        bash_command=f"python {TRANSFORM_LOAD_SCRIPT}",
    )

    t1 >> t2