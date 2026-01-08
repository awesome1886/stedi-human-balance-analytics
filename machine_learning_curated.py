import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# --- Node 1: Source - Step Trainer Trusted ---
step_trainer_trusted_node = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="step_trainer_trusted",
    transformation_ctx="step_trainer_trusted_node"
)

# --- Node 2: Source - Accelerometer Trusted ---
accelerometer_trusted_node = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="accelerometer_trusted",
    transformation_ctx="accelerometer_trusted_node"
)

# --- Node 3: Join ---
# Join on timestamp to match the exact moment of the step
# step_trainer.sensorreadingtime == accelerometer.timestamp
joined_node = Join.apply(
    frame1=step_trainer_trusted_node,
    frame2=accelerometer_trusted_node,
    keys1=["sensorreadingtime"],
    keys2=["timestamp"],
    transformation_ctx="joined_node"
)

# --- Node 4: Target - S3 Write ---
AmazonS3_node4 = glueContext.getSink(
    path="s3://johndoe-stedi-lakehouse-v3/machine_learning/curated/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="AmazonS3_node4"
)
AmazonS3_node4.setCatalogInfo(catalogDatabase="stedi", catalogTableName="machine_learning_curated")
AmazonS3_node4.setFormat("glueparquet")
AmazonS3_node4.writeFrame(joined_node)

job.commit()