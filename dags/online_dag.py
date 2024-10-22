# Set environment
import sys, os
sys.path.append(os.path.expanduser('~/code/IE212.O11.Group11'))

# Import libs
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

# Import custom modules
from _kafka.produce import produce_praw
from _spark.stream import structured_stream

default_args = {
	'owner': 'Group11',
	'start_date': datetime(2024, 1, 17, 10, 0),
	'retries': 5,
    'retry_delay': timedelta(minutes=10),
}

with DAG(
	dag_id='online_dag',
	default_args=default_args,
	schedule_interval='@daily'
) as dag:
    # Crawl data from reddit using praw api
    crawling=PythonOperator(
        task_id='crawl_data',
        python_callable=produce_praw,
    )

    # Stream and execute data using spark structured streaming
    streaming=PythonOperator(
        task_id='stream_data',
        python_callable=structured_stream,
    )
    
    # Main flow
    [crawling, streaming]