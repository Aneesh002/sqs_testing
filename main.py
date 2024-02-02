import json
import logging
from sqs_queue.client import get_sqs_client
from sqs_queue.message_operations import send_messages_batch
from sqs_queue.python_instance import ProcessingInstance
from api_operation.api_fetching_msg import fetch_api

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main():
    """


    :return:
    """
    aws_sqs_key_id = 'access-key'  #
    aws_sqs_secret_access_key = '-secret-key'  #
    region_name = 'your-region'
    sqs_queue_url = 'sqs-queue-url'  #
    api_url = 'http://api-endpoint'

    sqs_client = get_sqs_client(region_name=region_name, aws_sqs_key_id=aws_sqs_key_id,
                                aws_sqs_secret_access_key=aws_sqs_secret_access_key)
    processing_instance = ProcessingInstance()

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
        if len(messages_string) > 0:
            messages: list[dict] = json.loads(messages_string)  # 12

            if not processing_instance.state:
                first_message = messages.pop(0)  # first file's metadata
                while messages:
                    message_batch: list = [{"Id": str(index), "MessageBody": json.dumps(msg)} for index, msg in
                                           enumerate(messages[:10])]
                    messages = messages[10:]
                    send_messages_batch(sqs_client=sqs_client, queue_url=sqs_queue_url, batch=message_batch)
                processing_instance.state = True
                processing_instance.process_message(first_message)
            else:
                while messages:
                    message_batch: list = [{"Id": str(index), "MessageBody": json.dumps(msg)} for index, msg in
                                           enumerate(messages[:10])]
                    messages = messages[10:]
                    send_messages_batch(sqs_client=sqs_client, queue_url=sqs_queue_url, batch=message_batch)

            processing_instance.check_and_process_next_message(sqs_client, sqs_queue_url)
    except Exception as e:
        logger.error(f"An error occurred: {e}")


if __name__ == "__main__":
    main()

'''
 just an example to show what the fileinfo will hold the list of dictionaries


files_info = [
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
]

'''
