import sys
import boto3
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1703836628538 = glueContext.create_dynamic_frame.from_catalog(
    database="akshay_de_p1_db",
    table_name="order_table_csv",
    transformation_ctx="AWSGlueDataCatalog_node1703836628538",
)

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1703836629189 = glueContext.create_dynamic_frame.from_catalog(
    database="akshay_de_p1_db",
    table_name="return_table_csv",
    transformation_ctx="AWSGlueDataCatalog_node1703836629189",
)

# Script generated for node Join
Join_node1703836656519 = Join.apply(
    frame1=AWSGlueDataCatalog_node1703836628538,
    frame2=AWSGlueDataCatalog_node1703836629189,
    keys1=["col0"],
    keys2=["col0"],
    transformation_ctx="Join_node1703836656519",
)

# Script generated for node Drop Fields
DropFields_node1703836878481 = DropFields.apply(
    frame=Join_node1703836656519,
    paths=["`.col0`"],
    transformation_ctx="DropFields_node1703836878481",
)
Repartitioned_node1703836878481 = DropFields_node1703836878481.repartition(1)
# Script generated for node Amazon S3
AmazonS3_node1703836925470 = glueContext.write_dynamic_frame.from_options(
    frame=Repartitioned_node1703836878481,
    connection_type="s3",
    format="glueparquet",
    connection_options={"path": "s3://joined-bucket-de-p1", "partitionKeys": []},
    format_options={"compression": "snappy"},
    transformation_ctx="AmazonS3_node1703836925470",
)

job.commit()
