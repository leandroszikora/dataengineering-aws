import boto3
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def lambda_handler(event, context):
    events_to_process = event['body']['events_to_process']

    for event_to_process in events_to_process:
        key = f"s3://{event_to_process['bucket']}/{event_to_process['key']}"
        status = event_to_process['status']

        dynamodb = boto3.resource('dynamodb')
        table = dynamodb.Table('datasets-catalog')

        table.update_item(
            Key={'id': key},
            AttributeUpdates={'status': {'Value': status, 'Action': 'PUT'}}
        )

    return events_to_process
