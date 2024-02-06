def count_messages_in_queue(sqs_client, sqs_queue_url):
    """
    This function is responsible for counting the number of message that are
    available currently in the AWS sqs queue for implementing the logic to have
    a look whether SQS queue is empty or not

    :param sqs_client:(object) The SQS Client object configured for accessing the AWS SQS QUEUE
    :param sqs_queue_url:(str) The URL of the AWS SQS queue from which attributes messages is  to be received
    :return:(int)This function returns the integer value which is the no of approximate message in the AWS sqs queue

    """
    response = sqs_client.get_queue_attributes(
        QueueUrl=sqs_queue_url,
        AttributeNames=['ApproximateNumberOfMessages']
    )
    message_in_queue = int(response['Attributes']['ApproximateNumberOfMessages'])
    return message_in_queue
