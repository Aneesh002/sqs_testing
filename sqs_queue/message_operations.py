import json
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()


def send_messages_batch(sqs_client, queue_url, batch):
    """

    :param sqs_client:
    :param queue_url:
    :param batch:
    :return:
    """
    try:
        response = sqs_client.send_message_batch(QueueUrl=queue_url, Entries=batch)  # We must pass the batch to Entries
        for success in response.get('Successful', []):
            logger.info(f"Message sent successfully: {success['MessageId']}")
        for failure in response.get('Failed', []):
            logger.error(f"Failed to send message: {failure['Id']}")
    # except ClientError as e:
    except Exception as e:
        logger.error(f"An error occurred: {e}")
        # raise


def receive_messages(sqs_client, queue_url, max_num):
    """
    :param sqs_client:
    :param queue_url:
    :param max_num:
    :return:
    """

    try:
        response = sqs_client.receive_message(QueueUrl=queue_url, MaxNumberOfMessages=max_num, WaitTimeSeconds=20)
        message = response.get('Messages', [])
        if message:
            # Deserialize the message body from JSON string to Python dictionary
            message = message[0]
            message_body: dict = json.loads(message['Body'])
            return message['ReceiptHandle'], message_body
        return 'None', None
    except Exception as e:
        logger.info(f"Error receiving message: {e}")
        raise


def delete_message(sqs_client, queue_url, receipt_handle):
    try:
        response = sqs_client.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt_handle)
        return response
    except Exception as e:
        logger.info(f"Error deleting message: {e}")
        raise
