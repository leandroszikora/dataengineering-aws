"""
Kinesis ingestion example.

Needs to be called with an environment variable AWS_PROFILE to change the default behavior.
"""
import boto3
import json
import uuid
import random
import logging

from argparse import ArgumentParser, Namespace
from typing import Dict, Any


def main(stream_name: str, n_records: int) -> None:
    logger = logging.getLogger(__name__)
    kinesis = boto3.client('kinesis', region_name='us-east-1')  # region_name could be changed
    for n in range(0, n_records):
        random_data: Dict[str, Any] = dict(
            event_name=random.choice(['click', 'popup', 'hover']),
            value=int(random.random() * 10)
        )
        user_id: str = str(uuid.uuid4())

        response: Dict[str, Any] = kinesis.put_record(
            StreamName=stream_name,
            Data=json.dumps(random_data),
            PartitionKey=user_id
        )
        
        logger.info(response)


if __name__ == '__main__':
    parser: ArgumentParser = ArgumentParser(description=__doc__)
    parser.add_argument('records', metavar='r', type=int, nargs=1,
                        help="Number of records to put into kinesis data stream")
    parser.add_argument('-s', dest='stream', type=str, help="Name of the kinesis data stream")

    args: Namespace = parser.parse_args()
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    main(args.stream, args.records[0])
