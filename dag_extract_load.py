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
        "retry_delay": timedelta(minutes=5),
        # 'queue': 'bash_queue',
        # 'pool': 'backfill',
        # 'priority_weight': 10,
        # 'end_date': datetime(2016, 1, 1),
        # 'wait_for_downstream': False,
        # 'sla': timedelta(hours=2),
        # 'execution_timeout': timedelta(seconds=300),
        # 'on_failure_callback': some_function,
        # 'on_success_callback': some_other_function,
        # 'on_retry_callback': another_function,
        # 'sla_miss_callback': yet_another_function,
        # 'trigger_rule': 'all_success'
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
        bash_command="python /home/sal/PROJEKTY_CV/world_holidays/extract.py --country CZ --year 2024",
    )

    t2 = BashOperator(
        task_id="load",
        bash_command="python /home/sal/PROJEKTY_CV/world_holidays/transform_load.py --source_blob CZ_2024.json ",
    )

    t1 >> t2