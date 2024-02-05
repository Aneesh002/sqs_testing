import json
import logging
from sqs_queue.client import get_sqs_client
from sqs_queue.message_operations import send_messages_batch
from sqs_queue.python_instance import ProcessSQSQueue
from api_operation.api_fetching_msg import fetch_api

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main():

    aws_sqs_key_id = 'access-key'  #
    aws_sqs_secret_access_key = '-secret-key'  #
    region_name = 'your-region'
    sqs_queue_url = 'sqs-queue-url'  #
    api_url = 'http://api-endpoint'

    sqs_client = get_sqs_client(region_name=region_name, aws_sqs_key_id=aws_sqs_key_id,
                                aws_sqs_secret_access_key=aws_sqs_secret_access_key)
    sqs_queue_instance = ProcessSQSQueue()

    try:

        messages_string = fetch_api(api_url)
        '''
            " [
                {
                    'guid': 'f9d8f7e6d5c4b3a2',  
                    'filepath': 's3://my-bucket/document1.pdf',
                    'filename': 'document1.pdf'
                },
                {
                    'guid': 'a1b2c3d4e5f6g7h8',  
                    'filepath': 's3://my-bucket/image2.jpg',
                    'filename': 'image2.jpg'
                },
                {
                    'guid': '1a2b3c4d5e6f7g8h',  
                    'filepath': 's3://my-bucket/report3.txt',
                    'filename': 'report3.txt'
                }
            ]"

        '''
        if len(messages_string):
            messages: list[dict] = json.loads(messages_string)

            if not sqs_queue_instance.running_state:
                first_message = messages.pop(0)  # first file's metadata
                send_to_sqs(sqs_client=sqs_client, sqs_queue_url=sqs_queue_url, messages=messages)
                sqs_queue_instance.running_state = True
                # update the status = processing of the exact file in the MongoDB.
                sqs_queue_instance.process_message(first_message)
                sqs_queue_instance.running_state = False
            else:
                send_to_sqs(sqs_client=sqs_client, sqs_queue_url=sqs_queue_url, messages=messages)
            sqs_queue_instance.fetch_sqs_message(sqs_client, sqs_queue_url)
        else:
            pass
    except Exception as e:
        logger.error(f"An error occurred: {e}")


def send_to_sqs(sqs_client, sqs_queue_url, messages):
    """
    This function is responsible for taking a list of messages and then send to the AWS SQS QUEUE in the batch up to 10
    messages each .

    :param sqs_client:(obj)Initialized client object for accessing the AWS SQS queue.
    :param sqs_queue_url:(str)The URL of the AWS SQS queue from which for sending messages to the AWS SQS QUEUE
    :param messages:list of messages that is divided into the batch and then sends to the SQS QUEUE
    :return:None , This function does not return a  value
    """

    while messages:
        # message_batch = []
        # for index, msg in enumerate(messages[:10]):
        #     message_batch.append({"Id": str(index), "MessageBody": json.dumps(msg)})
        # update the MongoDB with the list of sent file with status=pending.
        message_batch: list = [{"Id": str(index), "MessageBody": json.dumps(msg)} for index, msg in
                               enumerate(messages[:10])]
        messages = messages[10:]
        send_messages_batch(sqs_client=sqs_client, queue_url=sqs_queue_url, batch=message_batch)


if __name__ == "__main__":
    main()


