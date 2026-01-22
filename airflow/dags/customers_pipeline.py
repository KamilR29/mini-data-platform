from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {"owner": "airflow", "depends_on_past": False}

with DAG(
    dag_id="customers_pipeline",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    bronze_to_silver = BashOperator(
        task_id="bronze_to_silver_customers",
        bash_command=r"""
        docker exec spark-master /opt/spark/bin/spark-submit \
          --conf spark.jars.ivy=/tmp/ivy \
          --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
          /work/spark/bronze_to_silver_customers.py
        """,
    )

    ge_validate_silver = BashOperator(
        task_id="ge_validate_customers_silver",
        bash_command=r"""
        python /work/qa/ge_validate_customers.py
        """,
    )

    silver_to_gold = BashOperator(
        task_id="silver_to_gold_customers",
        bash_command=r"""
        docker exec spark-master /opt/spark/bin/spark-submit \
          --conf spark.jars.ivy=/tmp/ivy \
          --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
          /work/spark/silver_to_gold_customers.py
        """,
    )

    bronze_to_silver >> ge_validate_silver >> silver_to_gold
