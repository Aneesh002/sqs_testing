import logging
import time
from message_operations import receive_sqs_message, sqs_delete_message


class ProcessSQSQueue:
    def __init__(self):
        self.running_state = False

    def process_message(self, message):
        """
        When the message is here to get processed, we need to check if any error occurs while in the processing steps.
        if there is any error, we have to update the MongoDB of the same file with the status of the error.
        :param message:(dict) This is the individual message that is passed to processing instance
        :return:None , This function does not return a value but display the information of processing message with
        """
        print(f"Processing message: {message['content']}")
        time.sleep(4)  # processing time

    def fetch_sqs_message(self, sqs_client, queue_url):
        """
        Continuously fetches and processes messages from an AWS SQS queue until the queue is empty.
        This function polls the SQS queue for message ,delete the message after  receiving each  message
        from the SQS QUEUE and then processes each message.Loop continue until no message are available in the QUEUE

        :param sqs_client:(obj)Initialized client object for accessing the AWS SQS queue.
        :param queue_url:(str)The URL of the AWS SQS queue from which to fetch messages.

        :return:None ,This function does not return a value but logs information regarding the  processing state and
        the errors
        """

        while True:
            receipt_handle, message_id, message_body = receive_sqs_message(sqs_client=sqs_client, queue_url=queue_url, max_num=1)
            if message_body is not None:
                response = sqs_delete_message(sqs_client, queue_url, receipt_handle)  # we need to check if the message will
                # immediately get deleted or not.
                self.running_state = True
                self.process_message(message_body)
                self.running_state = False

            else:
                if receipt_handle is None:
                    logging.info(f"INFO [Error while fetching the message from AWS SQS Queue.]")
                else:
                    logging.info(f"INFO [{receipt_handle}]")
                    # need to work if the sqs queue is empty.
                break
                # return False

