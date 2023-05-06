from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
import os

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

    env_vars = os.environ.copy()
    env_vars['WH_CONFIG'] = '/home/sal/PROJEKTY_CV/world_holidays/pipeline.conf'

    t1 = BashOperator(
        task_id="extract",
        bash_command="python /home/sal/PROJEKTY_CV/world_holidays/extract.py",
        env=env_vars
    )

    t2 = BashOperator(
        task_id="load",
        bash_command="python /home/sal/PROJEKTY_CV/world_holidays/transform_load.py",
        env=env_vars
    )

    t1 >> t2