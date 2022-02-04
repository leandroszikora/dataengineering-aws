import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions

from pyspark.context import SparkContext
from pyspark.sql.functions import regexp_replace, trim, col, lower, split, explode


class WordCounterETL:
    def __init__(self, spark):
        self.spark = spark

    def read_from_table(self):
        query = "SELECT cord_uid, dt_month, abstract FROM datapath_health_stage.medical_research_curated"
        return self.spark.sql(query)

    @staticmethod
    def remove_punctuation(df_input):
        return df_input \
            .withColumn('abstract_wop', lower(trim(regexp_replace(col('abstract'), '\\p{Punct}', '')))) \
            .drop('abstract')

    @staticmethod
    def explode_split_abstract(df_input):
        return df_input \
            .withColumn('word', explode(split(col('abstract_wop'), '[\s]+'))) \
            .drop('abstract_wop') \
            .where("word != ''")

    @staticmethod
    def count_words(df_input):
        return df_input \
            .groupBy('cord_uid', 'dt_month', 'word') \
            .count()

    @staticmethod
    def save(df_to_save):
        df_to_save.write \
            .mode('overwrite') \
            .format('parquet') \
            .option('compression', 'snappy') \
            .partitionBy('dt_month') \
            .save('s3://<BUCKET>/analytics/medicalresearch_word_count/')

    def execute(self):
        df = self.read_from_table()
        # Step 1: Remove punctuation from abstract column 
        df = self.remove_punctuation(df)
        # Step 2: Explode abstract column using split function
        df = self.explode_split_abstract(df)
        # Step 3: Obtain word count grouping by pk and word
        df = self.count_words(df)
        # Step 4: Save dataframe as table
        self.save(df)


if __name__ == '__main__':
    args = getResolvedOptions(sys.argv, ['JOB_NAME'])

    sc = SparkContext()
    glueContext = GlueContext(sc)
    job = Job(glueContext)

    spark_session = glueContext.spark_session

    job.init(args['JOB_NAME'], args)

    word_counter_etl = WordCounterETL(spark_session)
    word_counter_etl.execute()

    job.commit()
