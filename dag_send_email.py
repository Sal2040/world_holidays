from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
import os

with DAG(
    dag_id="wh_send_email",
    default_args={
        "depends_on_past": False,
        "email": ["sal2040.dev@gmail.com"],
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 0,
        "retry_delay": timedelta(minutes=1),
    },
    description="send_holidays_email",
    # send email every Friday at 12:00:
    schedule="0 12 * * 5",
    start_date=datetime(2023, 4, 1),
    catchup=False,
    tags=["wh"],
    ) as dag:

    env_vars = os.environ.copy()
    env_vars['WH_CONFIG'] = '/home/sal/PROJEKTY_CV/world_holidays/pipeline.conf'

    t1 = BashOperator(
        task_id="extract",
        bash_command="python /home/sal/PROJEKTY_CV/world_holidays/send_email.py",
        env=env_vars
    )

    t1