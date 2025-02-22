from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from datetime import datetime

dag = DAG(
    "mlops_csv_pipeline",
    schedule_interval="@daily",
    start_date=datetime(2024, 2, 20),
    catchup=False,
)

process_with_dataflow = BashOperator(
    task_id="process_with_dataflow",
    bash_command="python /Data_Pipeline/scripts/dataflow_processing.py",
    dag=dag,
)

load_to_bigquery = GCSToBigQueryOperator(
    task_id="load_to_bigquery",
    bucket="ai_chatbot_seller_central",
    source_objects=["new_data_sentiment.csv"],
    destination_project_dataset_table="mlops_dataset.cleaned_reviews",
    write_disposition="WRITE_APPEND",
    dag=dag,
)

process_with_dataflow >> load_to_bigquery
