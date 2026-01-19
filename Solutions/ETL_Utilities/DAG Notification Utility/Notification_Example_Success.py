import requests
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.notifications.basenotifier import BaseNotifier
import pytz

import pendulum


# Define your webhook URL


def send_webex_notification(date, job_name, message_text):
    # Construct the message payload
    payload = {
        'text': 'Hello, this is a message from your webhook!',
        'markdown': f"This is a Job Alert Notification Message: at {date}, The {job_name}, {message_text}"
    }

    # Send the message to the Webex space using a webhook
    webhook_url = 'https://webexapis.com/v1/webhooks/incoming/Y2lzY29zcGFyazovL3VzL1dFQkhPT0svMTEwZTczMGYtMDMzOS00M2UxLTgwZmMtNzYzNjJjMzhiOTNm'
    response = requests.post(webhook_url, json=payload)

    if response.status_code == 0:
        print("Webex notification sent successfully!")
    else:
        print(f"Error sending Webex notification: {response.text}")


def on_success_alert(context):
    job_name = context['task_instance'].dag_id
    date = datetime.now(pytz.timezone('America/Chicago'))
    unique_message = f"Succeeded!\nPlease see for additional details:"
    send_webex_notification(date, job_name, unique_message)


def on_skipped_callback(context):
    job_name = context['task_instance'].dag_id
    date = datetime.now(pytz.timezone('America/Chicago'))
    skip_message = "Skipped! \n Please see for additional details:"
    send_webex_notification(date, job_name, skip_message)


def on_failure_callback(context):
    job_name = context['task_instance'].dag_id
    date = datetime.now(pytz.timezone('America/Chicago'))
    unique_message = f"Failed!\nPlease see for additional details:"
    send_webex_notification(date, job_name, unique_message)


def on_retry_callback():
    job_name = "Care Management Job"
    date = datetime.now(pytz.timezone('America/Chicago'))
    unique_message = f"Job unsuccessful, retrying!\nDAG ID: {job_name}\nExecution Date: {date}"
    send_webex_notification(date, job_name, unique_message)


def on_execute_callback(context):
    job_name = context['task_instance'].dag_id
    date = datetime.now(pytz.timezone('America/Chicago'))
    unique_message = f"has Started!\nPlease see for additional details:"
    send_webex_notification(date, job_name, unique_message)


default_args = dict(owner='airflow', depends_on_past=False, email_on_failure=False, email_on_retry=False, retries=1,
                    retry_delay=timedelta(minutes=5))

dag = DAG(
    'DAG_Success_Example',
    default_args=default_args,
    schedule_interval=None,  # This DAG is triggered manually
    start_date=days_ago(1),
    catchup=False,
    on_success_callback=on_success_alert,
    on_failure_callback=on_failure_callback,

)


def task1():
    print("Executing Task 1")
    # If the task is successful, return 'SUCCESS'
    return "SUCCESS"


def task2():
    print("Executing Task 2")
    # If the task is successful, return 'SUCCESS'
    return "SUCCESS"


task_1 = PythonOperator(
    task_id='task_1',
    python_callable=task1,
    dag=dag,
    on_execute_callback=on_execute_callback,
    on_retry_callback=on_retry_callback,
)

task_2 = PythonOperator(
    task_id='task_2',
    python_callable=task2,
    dag=dag,
    execution_timeout=timedelta(minutes=2),
    on_retry_callback=on_retry_callback,

)
task_1 >> task_2

