from prefect import flow, task, get_run_logger
import boto3
import time

UVA_ID = "kfm8nx"
PLATFORM = "prefect"
QUEUE_URL = f"https://sqs.us-east-1.amazonaws.com/440848399208/{UVA_ID}"
SUBMIT_QUEUE_URL = "https://sqs.us-east-1.amazonaws.com/440848399208/dp2-submit"
EXPECTED_UNIQUE = 21

# Create SQS client (reads AWS credentials from aws configure)
sqs = boto3.client("sqs", region_name="us-east-1")

# Task 1: Get queue status
@task
def get_queue_counts():
    """
    Checks the SQS queue and prints how many messages are visible, delayed, or hidden.
    """
    logger = get_run_logger()
    response = sqs.get_queue_attributes(QueueUrl=QUEUE_URL, AttributeNames=["All"])
    attrs = response.get("Attributes", {})
    visible = attrs.get("ApproximateNumberOfMessages", "0")
    delayed = attrs.get("ApproximateNumberOfMessagesDelayed", "0")
    not_visible = attrs.get("ApproximateNumberOfMessagesNotVisible", "0")
    logger.info(f"Queue counts — visible={visible}, not_visible={not_visible}, delayed={delayed}")
    return attrs


# Task 2: Receive and delete messages
@task
def receive_messages():
    """
    Receives messages from my queue one by one, reads their order number and word,
    deletes them after saving, and stops once all 21 unique pieces are collected.
    """
    logger = get_run_logger()
    collected = {}

    while len(collected) < EXPECTED_UNIQUE:
        response = sqs.receive_message(
            QueueUrl=QUEUE_URL,
            MaxNumberOfMessages=1,
            WaitTimeSeconds=10,
            MessageAttributeNames=["All"]
        )
        messages = response.get("Messages", [])

        if not messages:
            logger.info(f"No messages yet ({len(collected)}/{EXPECTED_UNIQUE})... waiting...")
            time.sleep(4)
            continue

        msg = messages[0]
        receipt = msg["ReceiptHandle"]
        attrs = msg.get("MessageAttributes", {})

        order_no = attrs.get("order_no", {}).get("StringValue")
        word = attrs.get("word", {}).get("StringValue")

        if order_no and word and int(order_no) not in collected:
            collected[int(order_no)] = word
            logger.info(f"Collected #{order_no}: '{word}' ({len(collected)}/{EXPECTED_UNIQUE})")

        # delete after processing
        sqs.delete_message(QueueUrl=QUEUE_URL, ReceiptHandle=receipt)

    return sorted(collected.items())


# Task 3: Reassemble phrase
@task
def build_phrase(pairs):
    """
    Joins all collected words in order to form the final phrase.
    """
    phrase = " ".join(word for _, word in pairs)
    get_run_logger().info(f"Final phrase: {phrase}")
    return phrase


# Task 4: Submit result
@task
def submit_solution(phrase):
    """
    Sends the final phrase back to the grading SQS queue with my ID and platform.
    """
    logger = get_run_logger()
    response = sqs.send_message(
        QueueUrl=SUBMIT_QUEUE_URL,
        MessageBody=f"dp2 submission for {UVA_ID}",
        MessageAttributes={
            "uvaid": {"DataType": "String", "StringValue": UVA_ID},
            "platform": {"DataType": "String", "StringValue": PLATFORM},
            "phrase": {"DataType": "String", "StringValue": phrase}
        }
    )
    status = response["ResponseMetadata"]["HTTPStatusCode"]
    if status == 200:
        logger.info("Submission acknowledged (HTTP 200).")
    else:
        logger.error(f"Submission failed: {status}")


# Flow definition
@flow(name="dp2-prefect-flow")
def dp2_flow():
    """
    Prefect pipeline to:
    1. Check queue status
    2. Receive and store all messages
    3. Build the final phrase
    4. Submit phrase to grading queue
    """
    get_queue_counts()
    pairs = receive_messages()
    phrase = build_phrase(pairs)
    submit_solution(phrase)
    get_run_logger().info("Flow complete!")


# Run the flow
if __name__ == "__main__":
    dp2_flow()
