from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.bash import BashOperator
from datetime import timedelta

default_args = {
    "owner": "miayoubi",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1)
}

with DAG(
    dag_id="spark_jobs_scheduler",
    default_args=default_args,
    start_date=days_ago(0),
    schedule_interval="@hourly",
    catchup=False,
) as dag:

    # -------------------------------------
    # Start Spark Streaming Job
    # -------------------------------------
    start_spark_stream = BashOperator(
        task_id="start_streaming_job",
        bash_command="""
        docker exec spark-master /opt/spark/bin/spark-submit \
        --master spark://spark-master:7077 \
        --jars /opt/app/jars/spark-sql-kafka-0-10_2.12-3.4.1.jar,\
/opt/app/jars/spark-token-provider-kafka-0-10_2.12-3.4.1.jar,\
/opt/app/jars/kafka-clients-3.4.0.jar,\
/opt/app/jars/commons-pool2-2.11.1.jar,\
/opt/app/jars/mongo-spark-connector_2.12-10.1.1.jar,\
/opt/app/jars/mongodb-driver-sync-4.11.1.jar,\
/opt/app/jars/mongodb-driver-core-4.11.1.jar,\
/opt/app/jars/bson-4.11.1.jar \
        /opt/spark/jobs/spark_stream_orders.py
        """
    )

    # -------------------------------------
    # Stop Streaming Job
    # -------------------------------------
    stop_spark_stream = BashOperator(
        task_id="stop_streaming_job",
        bash_command="docker exec spark-master pkill -f spark_stream_orders.py || true"
    )

    # -------------------------------------
    # Run Mongo → HDFS Batch Job
    # -------------------------------------
    mongo_to_hdfs = BashOperator(
        task_id="run_mongo_to_hdfs",
        bash_command="""
        docker exec spark-master /opt/spark/bin/spark-submit \
        --master spark://spark-master:7077 \
        --jars /opt/app/jars/mongo-spark-connector_2.12-10.1.1.jar,\
/opt/app/jars/mongodb-driver-sync-4.11.1.jar,\
/opt/app/jars/mongodb-driver-core-4.11.1.jar,\
/opt/app/jars/bson-4.11.1.jar \
        /opt/spark/jobs/mongo_to_hdfs.py
        """
    )

    # -------------------------------------
    # Restart Streaming Job
    # -------------------------------------
    restart_stream = BashOperator(
        task_id="restart_streaming_job",
        bash_command=start_spark_stream.bash_command
    )

    # Dependencies
    start_spark_stream >> stop_spark_stream >> mongo_to_hdfs >> restart_stream
