import boto3
import logging
import json

from datetime import datetime, timedelta

STATE_MACHINE_ARN = "arn:aws:states:us-east-1:<YOUR_ACCOUNT_ID>:stateMachine:StageToAnalytics-SM"
QUEUE_NAME = "json-pending-files.fifo"

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def start_state_machine(client, item):
    client.start_execution(
        stateMachineArn=STATE_MACHINE_ARN,
        input=json.dumps(item)
    )


def delete_messages_from_queue(queue, messages):
    messages_to_delete = list(map(lambda x: dict(Id=x.message_id, ReceiptHandle=x.receipt_handle), messages))
    queue.delete_messages(Entries=messages_to_delete)


def parse_message_to_event(message):
    body = json.loads(message.body)
    return {
        'bucket': body['bucket'],
        'key': body['key'],
        'status': 'step_2'
    }


def lambda_handler(event, context):
    messages_well = []
    response = {}

    days_ago = int(event['days_ago'])
    required_files = event['required_files']
    job_name = event['job_name']
    dataset = event['dataset']

    event_date = (datetime.now() - timedelta(days=days_ago)).strftime("%Y%m%d")

    step_functions = boto3.client('stepfunctions')
    sqs = boto3.resource('sqs')
    queue = sqs.get_queue_by_name(QueueName=QUEUE_NAME)

    messages = queue.receive_messages(MaxNumberOfMessages=5)
    logger.info(f"Messages received: {len(messages)}")

    for message in messages:
        to_process_key = json.loads(message.body)['processed_key']
        data_date, file_name = to_process_key.split('/')[-2:]

        if data_date == event_date and file_name in required_files:
            messages_well.append(message)

    if len(messages_well) == len(required_files):
        events_to_process = list(map(parse_message_to_event, messages_well))
        response = {
            'status_code': 200,
            'body': {
                'event_datetime': datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
                'job_name': job_name,
                'source': f"s3://<BUCKET>/stage/pre-stage/{dataset}/{event_date}",
                'output': f"s3://<BUCKET>/stage/post-stage/{dataset}/{event_date}",
                'events_to_process': events_to_process
            }
        }
        start_state_machine(step_functions, response)
        delete_messages_from_queue(queue, messages_well)
    else:
        response = {
            'status_code': 400
        }

    logger.info(response)

    return response
