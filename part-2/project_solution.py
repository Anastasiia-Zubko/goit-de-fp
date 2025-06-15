from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

# DAG
default_args = {
    'start_date': datetime(2024, 6, 1),
}

with DAG(
    dag_id='etl_datalake_pipeline',
    schedule=None,
    catchup=False,
    default_args=default_args
) as dag:

    # Bronze: load CSV
    landing_to_bronze = BashOperator(
        task_id='landing_to_bronze',
        bash_command='cd /Users/anastasiiazubko/PycharmProjects/goit-de-fp/part-2 && python3 landing_to_bronze.py'
    )

    # Silver: clean data
    bronze_to_silver = BashOperator(
        task_id='bronze_to_silver',
        bash_command='cd /Users/anastasiiazubko/PycharmProjects/goit-de-fp/part-2 && python3 bronze_to_silver.py'
    )

    # Gold: aggregate data
    silver_to_gold = BashOperator(
        task_id='silver_to_gold',
        bash_command='cd /Users/anastasiiazubko/PycharmProjects/goit-de-fp/part-2 && python3 silver_to_gold.py'
    )

    # Set task order
    landing_to_bronze >> bronze_to_silver >> silver_to_gold
