import json
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()


def send_messages_batch(sqs_client, queue_url, batch):
    """
    This function is responsible to send the message in the batch of 10 messages as an entries to the sqs queue.

    :param sqs_client: (obj) Initialized client object for accessing the AWS SQS queue.
    :param queue_url: The URL of the AWS SQS queue from which to fetch messages.
    :param batch: This is the batch of 10 messages that is for sending to the AWS SQS QUEUE
    :return: None ,This function does not return value but logs the information of processing i.e. success or errors.
    """
    try:
        response = sqs_client.send_message_batch(QueueUrl=queue_url, Entries=batch)  # We must pass the batch to Entries
        for success in response.get('Successful', []):
            # MangoDB update
            logger.info(f"Message sent successfully: {success['MessageId']}")
        for failure in response.get('Failed', []):
            logger.error(f"Failed to send message: {failure['Id']}")
            # MangoDB update

    # except ClientError as e:
    except Exception as e:
        logger.error(f"An error occurred: {e}")


def receive_sqs_message(sqs_client, queue_url, max_num):
    """
    This function is responsible for receiving  as specified no of the maximum message i.e 1 as defined ,
    from the AWS SQS QUEUE ,It waits for the messages up to 20 sec after if it does not receive within that time
    then moves to next steps. If a message is received, it extracts the message body,
    deserializes it from a JSON string to a Python dictionary, and logs the message ID

    :param sqs_client: (object) The SQS Client object configured for accessing the AWS SQS QUEUE
    :param queue_url:(str) The URL of the AWS SQS queue from which messages are to be received.
    :param max_num: The maximum number of messages to receive from the queue in one call.
    :return: tuple,A tuple containing the receipt handle, message ID, and the message body as a dictionary if a message is
            successfully received. If the queue is empty, returns a message indicating the empty queue, `None` for
            the message ID, and `None` for the message body. In case of an exception, returns `None` for all three tuple elements.
    """

    try:
        response = sqs_client.receive_message(QueueUrl=queue_url, MaxNumberOfMessages=max_num, WaitTimeSeconds=20)
        message = response.get('Messages', [])
        if message:
            # Deserialize the message body from JSON string to Python dictionary
            message = message[0]
            message_body: dict = json.loads(message['Body'])
            logger.info(f"Message id {message['id']} fetched successfully.")
            return message['ReceiptHandle'], message['id'], message_body  # message['ReceiptHandle'] is string type.
        return "The AWS SQS Queue is empty", None, None
    except Exception as e:
        logger.info(f"Error receiving message: {e}")
        return None, None, None


def sqs_delete_message(sqs_client, queue_url, receipt_handle):
    """
    This function is responsible for deleting the message from the AWS SQS QUEUE Using the message's receipt handle .

    :param sqs_client:(str)The SQS client object configured for accessing Amazon SQS which have permissions to delete
                     messages from the specified SQS queue.
    :param queue_url:(str) The URL of the SQS queue from which the message is to be deleted.
    :param receipt_handle: The receipt handle of the message to be deleted which is a unique identifier for the
                    receipt of the message by the client from the queue and is different from the message ID.

    :return: (Dict),The response from the SQS service upon successful deletion of the message. # need to check after
            the actual implementation
    """
    try:
        response = sqs_client.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt_handle)
        # logging.info(f'Deleted the message {message_id} from the AWS SQS Queue.')
        return response
    except Exception as e:
        logger.info(f"Error while deleting message: {e}")

