import json
import logging
from sqs_queue.client import get_sqs_client
from sqs_queue.message_operations import send_messages_batch
from sqs_queue.python_instance import ProcessSQSQueue
# from api_operation.api_fetching_msg import fetch_api
from sqs_queue.count_message_in_queue import count_messages_in_queue

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main():
    aws_access_key_id = 'AKIAVXUFWTYMZFAQRHSU'
    aws_secret_access_key = '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'
    region_name = 'us-east-1'
    QueueUrl= 'https://sqs.us-east-1.amazonaws.com/394343652889/vertex_ocr_queue.fifo'  #
    # api_url = 'http://api-endpoint'

    sqs_client = get_sqs_client(region_name=region_name, aws_access_key_id=aws_access_key_id,
                                aws_secret_access_key=aws_secret_access_key)

    sqs_queue_instance = ProcessSQSQueue()

    try:

        # messages_string = fetch_api(api_url)

        messages = [
            {"guid": "f9d8f7e6d5c4b3a2", "filepath": "s3://my-bucket/document1.pdf", "filename": "document1.pdf"},
            {"guid": "a1b2c3d4e5f6g7h8", "filepath": "s3://my-bucket/image2.jpg", "filename": "image2.jpg"},
            {"guid": "1a2b3c4d5e6f7g8h", "filepath": "s3://my-bucket/report3.txt", "filename": "report3.txt"},
            {"guid": "4444444444444444", "filepath": "s3://my-bucket/file4.txt", "filename": "file4.txt"},
            {"guid": "5555555555555555", "filepath": "s3://my-bucket/file5.txt", "filename": "file5.txt"},
            {"guid": "6666666666666666", "filepath": "s3://my-bucket/file6.txt", "filename": "file6.txt"},
            {"guid": "7777777777777777", "filepath": "s3://my-bucket/file7.txt", "filename": "file7.txt"},
            {"guid": "8888888888888888", "filepath": "s3://my-bucket/file8.txt", "filename": "file8.txt"},
            {"guid": "9999999999999999", "filepath": "s3://my-bucket/file9.txt", "filename": "file9.txt"},
            {"guid": "1010101010101010", "filepath": "s3://my-bucket/file10.txt", "filename": "file10.txt"},
            {"guid": "1111111111", "filepath": "s3://my-bucket/file11.txt", "filename": "file11.txt"},
            {"guid": "12121212121212121212121212121212", "filepath": "s3://my-bucket/file12.txt",
             "filename": "file12.txt"}
        ]

        if len(messages):
            # messages = json.loads(messages_string)

            message_count = count_messages_in_queue(sqs_client, QueueUrl)

            if not sqs_queue_instance.running_state and message_count == 0:
                first_message = messages.pop(0)  # first file's metadata
                send_to_sqs(sqs_client=sqs_client, QueueUrl=QueueUrl, messages=messages)
                sqs_queue_instance.running_state = True
                # update the status = processing of the exact file in the MongoDB.
                sqs_queue_instance.process_message(first_message)
                sqs_queue_instance.running_state = False
            else:
                send_to_sqs(sqs_client=sqs_client, QueueUrl=QueueUrl, messages=messages)
            sqs_queue_instance.fetch_sqs_message(sqs_client, QueueUrl)
        else:
            pass
    except Exception as e:
        logger.error(f"An error occurred: {e}")


def send_to_sqs(sqs_client, QueueUrl, messages):
    """
    This function is responsible for taking a list of messages and then send to the AWS SQS QUEUE in the batch up to 10
    messages each .

    :param sqs_client:(obj)Initialized client object for accessing the AWS SQS queue.
    :param QueueUrl:(str)The URL of the AWS SQS queue from which for sending messages to the AWS SQS QUEUE
    :param messages:list of messages that is divided into the batch and then sends to the SQS QUEUE
    :return:None , This function does not return a  value
    """
    message_counter = 1
    group_counter = 1
    while messages:
        current_batch = messages[:10]
        message_batch = [{
            "Id": str(message_counter + index),
            "MessageBody": json.dumps(msg),
            "MessageGroupId": f"Group{group_counter}",

        } for index, msg in enumerate(current_batch)]

        send_messages_batch(sqs_client, QueueUrl, message_batch)

        messages = messages[10:]
        message_counter += len(current_batch)
        group_counter += 1


if __name__ == "__main__":
    main()
