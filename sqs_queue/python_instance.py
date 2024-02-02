import time
from message_operations import delete_message, receive_messages


class ProcessingInstance:
    def __init__(self):
        self.state = False

    # def is_idle(self):
    #     return self.state == "paused"

    def process_message(self, message):
        # self.state = True
        print(f"Processing message: {message['content']}")
        time.sleep(4)  # processing time
        self.state = False

    def check_and_process_next_message(self, sqs_client, queue_url):
        """
        yesma chirnu vaneko: kita sqs queue naveye samma kaam garnu pareo | ki ta sqs queue khali xa vaye ko logic
        implement garnu pareo.
        :param sqs_client:
        :param queue_url:
        :return:
        """
        if not self.state:

            while True:

                receipt_handle, message_body = receive_messages(sqs_client=sqs_client, queue_url=queue_url, max_num=1)
                if message_body is not None:
                    if not self.state:
                        self.state = True
                        self.process_message(message_body)
                        delete_message(sqs_client, queue_url, receipt_handle)

                    # while sqs_queue:
                    if self.state == "paused":
                        messages = receive_messages(sqs_client=sqs_client, queue_url=queue_url, max_num=1)
                        if messages:
                            message = messages[0]
                            receipt_handle = message['ReceiptHandle']
                            self.state = "processing"
                            self.process_message(message)
                            delete_message(sqs_client, queue_url, receipt_handle)
                            self.state = "paused"
                        else:
                            # No messages to process, continue to pause
                            self.state = "paused"
                            time.sleep(1)
                    else:
                        time.sleep(1)

                else:
                    # need to work if the sqs queue is empty.
                    break



'''
import time

class processing_instance():
    def __init__(self):
        self.is_busy=False

    def process(self ,message ):
        message=receive_message()
        print(f"Processing message:" )
        self.is_busy =True
        time.sleep(10)
        self.is_busy = False


a=processing_instance()
'''
