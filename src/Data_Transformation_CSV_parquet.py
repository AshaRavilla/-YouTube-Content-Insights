import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from awsglue.dynamicframe import DynamicFrame

# Define and parse arguments
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Step 1: Read data from S3 (Amazon S3 node)
AmazonS3_node1725235354486 = glueContext.create_dynamic_frame.from_options(
    format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "optimizePerformance": False},
    connection_type="s3",
    format="csv",
    connection_options={"paths": ["s3://ashade-on-youtube-raw-useast1-dev/youtube/raw_statistics/"], "recurse": True},
    transformation_ctx="AmazonS3_node1725235354486"
)

# Convert DynamicFrame to DataFrame for easier manipulation
source_df = AmazonS3_node1725235354486.toDF()

# Step 2: Add 'region' column
# Extract 'region' from the file path
source_df = source_df.withColumn('region', F.regexp_extract(F.input_file_name(), 'region=([^/]+)', 1))

# Step 3: Apply schema changes (Change Schema node)
# Apply schema transformations
source_df = source_df.withColumn("categoryid", source_df["categoryid"].cast("bigint"))
# Apply transformation using the correct format
source_df = source_df.withColumn("trending_date", F.to_date(source_df["trending_date"], "dd.MM.yy"))
source_df = source_df.withColumn("likes", source_df["likes"].cast("bigint"))
source_df = source_df.withColumn("dislikes", source_df["dislikes"].cast("bigint"))
source_df = source_df.withColumn("comment_count", source_df["comment_count"].cast("bigint"))

# Convert back to DynamicFrame
transformed_dynamic_frame = DynamicFrame.fromDF(source_df, glueContext, "transformed_dynamic_frame")

# Step 4: Write data to S3 in Parquet format with partitioning and Snappy compression
sink = glueContext.write_dynamic_frame.from_options(
    frame=transformed_dynamic_frame,
    connection_type="s3",
    format="glueparquet",
    connection_options={
        "path": "s3://ashade-on-youtube-cleansed-useast1-dev/youtube/raw_statistics/",
        "partitionKeys": ["region"]  # Partition by 'region' only
    },
    format_options={"compression": "snappy"},
    transformation_ctx="sink"
)

# Commit the job
job.commit()
