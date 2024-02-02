import boto3
from botocore.exceptions import NoCredentialsError, ClientError
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()


def get_sqs_client(region_name, aws_sqs_key_id, aws_sqs_secret_access_key):
    """

    :param region_name:
    :param aws_sqs_key_id:
    :param aws_sqs_secret_access_key:
    :return:
    """

    if not (aws_sqs_key_id and aws_sqs_secret_access_key):
        raise ValueError("AWS access key and secret key must be provided")

    try:
        sqs_client = boto3.client(
            'sqs',
            region_name=region_name,
            aws_sqs_key_id=aws_sqs_key_id,
            aws_sqs_secret_access_key=aws_sqs_secret_access_key
        )
        logger.info("Successfully created SQS client.")
        return sqs_client
    except NoCredentialsError as e:
        logger.error("Credentials not available for AWS SQS: %s", e)
        return None
    except ClientError as e:
        logger.error("An error occurred while creating AWS SQS client: %s", e)
        return None
