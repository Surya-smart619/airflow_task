from datetime import datetime
import json

from airflow.decorators import task
from airflow.models.dag import DAG
from airflow.providers.amazon.aws.operators.s3 import S3ListOperator, Sequence
from airflow.providers.amazon.aws.sensors.sqs import SqsSensor
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.python import get_current_context

BUCKET_NAME = 'airflow-surya'
s3 = S3Hook(
    aws_conn_id='aws'
)


with DAG(
    dag_id='count_s3_lines',
    schedule=None,
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=["count-lines"],
) as dag:
    queue = SqsSensor(
        task_id='read_from_queue',
        aws_conn_id='aws',
        sqs_queue='S3-reader',
    )

    @task
    def fetch_file_names(messages, **kwargs):
        ti = kwargs['ti']
        messages = ti.xcom_pull(task_ids='read_from_queue', key='messages')
        filenames = []
        for message in messages:
            body = json.loads(message['Body'])
            for record in body["Records"]:
                filenames.append(record['s3']['object']['key'])
        return filenames

    @task
    def count_lines(filename):
        return {
            "filename": filename,
            "line_length": len(s3.read_key(filename, BUCKET_NAME).splitlines())
        }

    @task
    def write_s3(content):
        return s3.load_string(
            string_data=f'filename: {content["filename"]}\n line_length: {content["line_length"]}',
            key=f'output/{content["filename"]}',
            bucket_name=BUCKET_NAME
        )


    list_filenames = fetch_file_names(queue.output)
    counts_content = count_lines.expand(filename=list_filenames)
    write_counted_lines = write_s3.expand(content=counts_content)
