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
step_landing_node = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="step_trainer_landing", transformation_ctx="step_landing_node")
cust_curated_node = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customers_curated", transformation_ctx="cust_curated_node")

# Convert to Spark SQL
step_df = step_landing_node.toDF()
cust_df = cust_curated_node.toDF()
step_df.createOrReplaceTempView("step_trainer_landing")
cust_df.createOrReplaceTempView("customers_curated")

# Run SQL Join
query = """
SELECT step_trainer_landing.* 
FROM step_trainer_landing
JOIN customers_curated 
ON step_trainer_landing.serialnumber = customers_curated.serialnumber
"""
joined_df = spark.sql(query)
joined_node = DynamicFrame.fromDF(joined_df, glueContext, "joined_node")

# Write to S3
AmazonS3_node = glueContext.getSink(path="s3://YOUR_BUCKET_NAME/step_trainer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node")
AmazonS3_node.setCatalogInfo(catalogDatabase="stedi", catalogTableName="step_trainer_trusted")
AmazonS3_node.setFormat("glueparquet")
AmazonS3_node.writeFrame(joined_node)
job.commit()