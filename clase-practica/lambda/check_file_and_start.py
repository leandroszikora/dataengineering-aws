import boto3
import logging
import json

from botocore.exceptions import ClientError
from datetime import datetime
from urllib.parse import unquote_plus

STATE_MACHINE_ARN = "arn:aws:states:us-east-1:<YOUR_ACCOUNT_ID>:stateMachine:FlatJSONFile-SM"

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def parse_s3_event(s3_event):
    return {
        'bucket': s3_event['s3']['bucket']['name'],
        'key': unquote_plus(s3_event['s3']['object']['key'])
    }


def put_item(dynamo_table, item):
    try:
        dynamo_table.put_item(
            Item=item,
            ConditionExpression=f"attribute_not_exists(id)",
        )
    except ClientError as e:
        if e.response['Error']['Code'] == "ConditionalCheckFailedException":
            logger.info(e.response['Error']['Message'])
            logger.info("Key already exists in catalog")
        else:
            raise


def delete_item(table, key):
    try:
        response = table.delete_item(
            Key=key
        )
    except ClientError as e:
        logger.error('Fatal error', exc_info=True)
        raise e
    else:
        return response


def start_state_machine(client, item):
    client.start_execution(
        stateMachineArn=STATE_MACHINE_ARN,
        input=json.dumps(item)
    )


def lambda_handler(event, context):
    records = []
    
    step_functions = boto3.client('stepfunctions')
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table("datasets-catalog")

    logger.info(f"Checking {len(event['Records'])} records")

    for record in event['Records']:
        parsed_message = parse_s3_event(record)
        parsed_message['id'] = "s3://{bucket}/{key}".format(**parsed_message)

        logger.info(f"S3 operation: {record['eventName']}")

        if record['eventName'] == 'ObjectRemoved:Delete':
            delete_item(table, {'id': parsed_message['id']})
        else:
            parsed_message['size'] = record['s3']['object']['size'],
            parsed_message['last_modified_date'] = record['eventTime'].split('.')[0] + '+00:00',
            parsed_message['timestamp'] = int(round(datetime.utcnow().timestamp() * 1000, 0))
            parsed_message['status'] = 'new'

            put_item(table, parsed_message)
            records.append(parsed_message)

    response = {
        'status_code': 200,
        'body': {
            'event_datetime': datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
            'events_to_process': records
        }
    }
    logger.info(response)

    if len(records) != 0:
        start_state_machine(step_functions, response)

    return response
