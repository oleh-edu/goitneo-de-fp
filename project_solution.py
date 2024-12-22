import pendulum
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
}

dag = DAG(
    dag_id="athlete_data_pipeline",
    default_args=default_args,
    description="OLEG: ETL pipeline for athlete data",
    schedule=None,  # Updated from schedule_interval
    start_date=pendulum.today('UTC').add(days=-1),  # Updated from days_ago
    catchup=False,
)

# Spark configuration
#spark_default_conn = {
#    "host": "spark://217.61.58.159:7077",
#    "user": "",
#    "password": "",
#}

landing_to_bronze = SparkSubmitOperator(
    application='dags/oleg-tk/landing_to_bronze.py',
    task_id='landing_to_bronze',
    conn_id='spark-default',
    verbose=1,
    dag=dag,
#    conf=spark_default_conn,
)

bronze_to_silver = SparkSubmitOperator(
    application='dags/oleg-tk/bronze_to_silver.py',
    task_id='bronze_to_silver',
    conn_id='spark-default',
    verbose=1,
    dag=dag,
#    conf=spark_default_conn,
)

silver_to_gold = SparkSubmitOperator(
    application='dags/oleg-tk/silver_to_gold.py',
    task_id='silver_to_gold',
    conn_id='spark-default',
    verbose=1,
    dag=dag,
#    conf=spark_default_conn,
)

landing_to_bronze >> bronze_to_silver >> silver_to_gold