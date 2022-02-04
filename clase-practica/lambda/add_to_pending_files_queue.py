import boto3
import logging
import json

from datetime import datetime

QUEUE_NAME = "json-pending-files.fifo"

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def lambda_handler(event, context):
    events_to_process = event['body']['events_to_process']
    for event_to_process in events_to_process:
        sqs = boto3.resource('sqs')
        queue = sqs.get_queue_by_name(QueueName=QUEUE_NAME)
        queue.send_message(
            MessageBody=json.dumps(event_to_process),
            MessageGroupId=datetime.now().strftime("%Y%m%d")
        )

    return
