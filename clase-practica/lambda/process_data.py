import boto3
import datetime as dt
import json
import logging

logging.basicConfig()
logger = logging.getLogger(__name__)


def datetimeconverter(o):
    if isinstance(o, dt.datetime):
        return o.__str__()


def lambda_handler(event, context):
    """Calls custom transform developed by user

    Arguments:
        event {dict} -- Dictionary with details on previous processing step
        context {dict} -- Dictionary with details on Lambda context

    Returns:
        {dict} -- Dictionary with Processed Bucket and Key(s)
    """
    job_name = event['body']['job_name']
    source_location = event['body']['source']
    output_location = event['body']['output']

    try:
        logger.info('Fetching event data from previous step')
        client = boto3.client('glue')

        job_response = client.start_job_run(
            JobName=job_name,
            Arguments={
                '--JOB_NAME': job_name,
                '--SOURCE_LOCATION': source_location,
                '--OUTPUT_LOCATION': output_location
            },
            MaxCapacity=2.0
        )

        json_data = json.loads(json.dumps(
            job_response, default=datetimeconverter))

        job_details = {
            "jobName": job_name,
            "jobRunId": json_data.get('JobRunId'),
            "jobStatus": 'STARTED'
        }
        response = {
            'jobDetails': job_details
        }

    except Exception as e:
        logger.error("Fatal error", exc_info=True)
        raise e

    return response
