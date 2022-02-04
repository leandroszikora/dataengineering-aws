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
    try:
        logger.info('Fetching event data from previous step')
        job_details = event['body']['job']['jobDetails']
        client = boto3.client('glue')

        job_response = client.get_job_run(
            JobName=job_details['jobName'], RunId=job_details['jobRunId'])
        json_data = json.loads(json.dumps(
            job_response, default=datetimeconverter))

        # IMPORTANT update the status of the job based on the job_response (e.g RUNNING, SUCCEEDED, FAILED)
        job_details['jobStatus'] = json_data.get('JobRun').get('JobRunState')
        response = {
            'jobDetails': job_details
        }

    except Exception as e:
        logger.error("Fatal error", exc_info=True)
        raise e

    return response
