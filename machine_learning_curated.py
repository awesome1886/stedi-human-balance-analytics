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

# Read Tables
step_trusted_node = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="step_trainer_trusted", transformation_ctx="step_trusted_node")
accel_trusted_node = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_trusted", transformation_ctx="accel_trusted_node")

# Convert to Spark SQL
step_df = step_trusted_node.toDF()
accel_df = accel_trusted_node.toDF()
step_df.createOrReplaceTempView("step_trainer_trusted")
accel_df.createOrReplaceTempView("accelerometer_trusted")

# Run SQL Join
query = """
SELECT *
FROM step_trainer_trusted
JOIN accelerometer_trusted 
ON step_trainer_trusted.sensorreadingtime = accelerometer_trusted.timestamp
"""
joined_df = spark.sql(query)
joined_node = DynamicFrame.fromDF(joined_df, glueContext, "joined_node")

# Write to S3
AmazonS3_node = glueContext.getSink(path="s3://YOUR_BUCKET_NAME/machine_learning/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node")
AmazonS3_node.setCatalogInfo(catalogDatabase="stedi", catalogTableName="machine_learning_curated")
AmazonS3_node.setFormat("glueparquet")
AmazonS3_node.writeFrame(joined_node)
job.commit()