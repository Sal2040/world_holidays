from datetime import datetime, timedelta

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.bash import BashOperator
with DAG(
    dag_id="wh_extract_load",
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    default_args={
        "depends_on_past": False,
        "email": ["alwan.samer24@gmail.com"],
        "email_on_failure": True,
        "email_on_retry": True,
        "retries": 1,
        "retry_delay": timedelta(minutes=1),
    },
    description="world_holidays extract load DAG",
    schedule="0 0 2 4 *",
    start_date=datetime(2023, 4, 1),
    catchup=False,
    tags=["wh"],
) as dag:

    # t1, t2 and t3 are examples of tasks created by instantiating operators
    t1 = BashOperator(
        task_id="extract",
        bash_command="python /home/sal/PROJEKTY_CV/world_holidays/extract.py",
    )

    t2 = BashOperator(
        task_id="load",
        bash_command="python /home/sal/PROJEKTY_CV/world_holidays/transform_load.py",
    )

    t1 >> t2