import boto3
from botocore.exceptions import NoCredentialsError, ClientError
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()



def get_sqs_client(region_name, aws_access_key_id, aws_secret_access_key):
    """
    Creates and returns an AWS SQS client object using the specified credentials and region. This client
    is used for interacting with AWS SQS, including operations like sending and receiving messages from a
    specified AWS SQS QUEUE.

    :param region_name: (str) The name of the AWS region where the SQS service is located.
    :param aws_access_key_id: (str) The AWS access key ID, part of the credentials to authenticate with AWS.
    :param aws_secret_access_key: (str) The AWS secret access key, part of the credentials to authenticate with AWS.
    :return: A boto3 SQS client object configured with the provided credentials and region,
             ready for interacting with AWS SQS. Returns None if the client creation fails due to
             missing credentials or other errors.
    """

    try:
        sqs_client = boto3.client(
            'sqs',
            region_name=region_name,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key  # Corrected parameter name
        )
        logger.info("Successfully created SQS client.")
        return sqs_client
    except NoCredentialsError as e:
        logger.error("Credentials not available for AWS SQS: %s", e)
        return None
    except ClientError as e:
        logger.error("An error occurred while creating AWS SQS client: %s", e)