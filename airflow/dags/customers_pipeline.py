from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

DEFAULT_ARGS = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="customers_end_to_end",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2026, 1, 1),
    schedule="0 * * * *",
    catchup=False,
    tags=["mini-data-platform", "customers"],
) as dag:

    bronze_to_silver = BashOperator(
        task_id="bronze_to_silver_customers",
        bash_command=(
            "docker exec spark-master /opt/spark/bin/spark-submit "
            "--conf spark.jars.ivy=/tmp/ivy "
            "--packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 "
            "/work/spark/bronze_to_silver_customers.py"
        ),
    )

    silver_to_gold = BashOperator(
        task_id="silver_to_gold_customers",
        bash_command=(
            "docker exec spark-master /opt/spark/bin/spark-submit "
            "--conf spark.jars.ivy=/tmp/ivy "
            "--packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 "
            "/work/spark/silver_to_gold_customers.py"
        ),
    )

    train_model = BashOperator(
        task_id="mlflow_train_customers",
        bash_command="cd /work && python ml/train_customers.py",
    )

    bronze_to_silver >> silver_to_gold >> train_model
