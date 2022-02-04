import boto3
import json
import logging

from urllib.parse import unquote_plus

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def parse(json_data):
    l = []
    for d in json_data:
        o = d.copy()
        for k in d:
            if type(d[k]) in [dict, list]:
                o.pop(k)
        l.append(o)

    return l


def lambda_handler(event, context):
    events_to_process = event['body']['events_to_process']
    processed_events = []

    for event_to_process in events_to_process:
        bucket = event_to_process['bucket']
        key = event_to_process['key']

        s3_resource = boto3.resource('s3')
        s3_client = boto3.client('s3')

        logger.info('Downloading object: {}/{}'.format(bucket, key))
        object_path = '/tmp/' + key.split('/')[-1]
        key = unquote_plus(key)
        s3_resource.Bucket(bucket).download_file(key, object_path)

        with open(object_path, 'r') as raw_file:
            data = raw_file.read()

        json_data = json.loads(data)

        output_path = "{}_parsed.json".format(object_path.split('.')[0])
        with open(output_path, "w", encoding='utf-8') as write_file:
            json.dump(parse(json_data), write_file,
                      ensure_ascii=False, indent=4)

        s3_path = f'stage/pre-stage/{"/".join(key.split("/")[1:-1])}/{output_path.split("/")[-1]}'
        s3_client.upload_file(object_path, bucket, s3_path)

        event_to_process['status'] = 'step_1'
        event_to_process['processed_key'] = s3_path

        processed_events.append(event_to_process)

    return processed_events
