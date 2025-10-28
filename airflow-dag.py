""" I ran the following Airflow DAG locally, imported Variables for AWS credentials in the Airflow UI,
and it successfully completed the DP2 simple pipeline challenge. """


from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sdk import Variable
from datetime import datetime, timedelta
import boto3, time

# --- Configuration ---
UVA_ID = "kfm8nx"
ACCOUNT_ID = "440848399208"
REGION = "us-east-1"
QUEUE_URL = f"https://sqs.{REGION}.amazonaws.com/{ACCOUNT_ID}/{UVA_ID}"
SUBMIT_QUEUE_URL = f"https://sqs.{REGION}.amazonaws.com/{ACCOUNT_ID}/dp2-submit"
EXPECTED_UNIQUE = 21

# --- Default DAG settings ---
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 10, 8),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

# --- DAG setup ---
dag = DAG(
    "dp2_airflow_simple_pipeline",
    default_args=default_args,
    description="Fetch 21 messages, build phrase, and submit result",
    schedule=None,
    catchup=False,
)

# --- Step 1: Count messages ---
def get_queue_counts():
    sqs = boto3.client(
        "sqs",
        region_name=REGION,
        aws_access_key_id=Variable.get("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=Variable.get("AWS_SECRET_ACCESS_KEY"),
    )
    attrs = sqs.get_queue_attributes(QueueUrl=QUEUE_URL, AttributeNames=["All"])["Attributes"]
    print("Queue counts:", attrs)
    return attrs


# --- Step 2: Receive & delete messages ---
def receive_messages():
    sqs = boto3.client(
        "sqs",
        region_name=REGION,
        aws_access_key_id=Variable.get("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=Variable.get("AWS_SECRET_ACCESS_KEY"),
    )
    collected = {}
    while len(collected) < EXPECTED_UNIQUE:
        response = sqs.receive_message(
            QueueUrl=QUEUE_URL,
            MaxNumberOfMessages=10,
            WaitTimeSeconds=10,
            MessageAttributeNames=["All"],
        )
        messages = response.get("Messages", [])
        if not messages:
            print(f"No messages yet ({len(collected)}/{EXPECTED_UNIQUE})...")
            time.sleep(4)
            continue

        msg = messages[0]
        receipt = msg["ReceiptHandle"]
        attrs = msg.get("MessageAttributes", {})
        order_no = attrs.get("order_no", {}).get("StringValue")
        word = attrs.get("word", {}).get("StringValue")

        if order_no and word and int(order_no) not in collected:
            collected[int(order_no)] = word
            print(f"Collected #{order_no}: '{word}'")

        sqs.delete_message(QueueUrl=QUEUE_URL, ReceiptHandle=receipt)

    return sorted(collected.items())


# --- Step 3: Build phrase ---
def build_phrase(**context):
    pairs = context["ti"].xcom_pull(task_ids="receive_messages")
    phrase = " ".join(word for _, word in pairs)
    print("Final phrase:", phrase)
    return phrase


# --- Step 4: Submit solution ---
def submit_solution(**context):
    phrase = context["ti"].xcom_pull(task_ids="build_phrase")
    sqs = boto3.client(
        "sqs",
        region_name=REGION,
        aws_access_key_id=Variable.get("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=Variable.get("AWS_SECRET_ACCESS_KEY"),
    )
    response = sqs.send_message(
        QueueUrl=SUBMIT_QUEUE_URL,
        MessageBody=f"dp2 submission for {UVA_ID}",
        MessageAttributes={
            "uvaid": {"DataType": "String", "StringValue": UVA_ID},
            "platform": {"DataType": "String", "StringValue": "airflow"},
            "phrase": {"DataType": "String", "StringValue": phrase},
        },
    )
    print("Submitted. HTTP:", response["ResponseMetadata"]["HTTPStatusCode"])


# --- Tasks ---
t1 = PythonOperator(task_id="get_queue_counts", python_callable=get_queue_counts, dag=dag)
t2 = PythonOperator(task_id="receive_messages", python_callable=receive_messages, dag=dag)
t3 = PythonOperator(task_id="build_phrase", python_callable=build_phrase, dag=dag)
t4 = PythonOperator(task_id="submit_solution", python_callable=submit_solution, dag=dag)

t1 >> t2 >> t3 >> t4
