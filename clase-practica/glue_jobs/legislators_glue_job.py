import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

args = getResolvedOptions(
    sys.argv, ['JOB_NAME', 'SOURCE_LOCATION', 'OUTPUT_LOCATION'])
source = args['SOURCE_LOCATION']
destination = args['OUTPUT_LOCATION']

glueContext = GlueContext(SparkContext.getOrCreate())
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

fields_to_select = ['role', 'seats', 'org_name', 'type', 'sort_name', 'area_id', 'on_behalf_of_id', 'name',
                    'birth_date', 'organization_id', 'gender', 'classification', 'legislative_period_id', 'image',
                    'given_name', 'start_date', 'family_name', 'id', 'death_date', 'end_date']

persons = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ['{}/{}'.format(source, 'persons_parsed.json')]
    },
    format_options={
        "withHeader": False
    },
    transformation_ctx="path={}".format('persons_df')
)

memberships = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ['{}/{}'.format(source, 'memberships_parsed.json')]
    },
    format_options={
        "withHeader": False
    },
    transformation_ctx="path={}".format('memberships_df')
)

organizations = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ['{}/{}'.format(source, 'organizations_parsed.json')]
    },
    format_options={
        "withHeader": False
    },
    transformation_ctx="path={}".format('organizations_df')
).rename_field('id', 'org_id').rename_field('name', 'org_name')

history = Join.apply(organizations,
                     Join.apply(persons, memberships, 'id', 'person_id'),
                     'org_id', 'organization_id').drop_fields(['person_id', 'org_id'])

persons.toDF().write.mode("overwrite").parquet(
    '{}/persons/'.format(destination))
organizations.toDF().write.mode("overwrite").parquet(
    '{}/organizations/'.format(destination))
memberships.toDF().write.mode("overwrite").parquet(
    '{}//memberships/'.format(destination))

history.toDF().select(fields_to_select).write.mode("overwrite").parquet(
    '{}/history/'.format(destination), partitionBy=['org_name'])

job.commit()
