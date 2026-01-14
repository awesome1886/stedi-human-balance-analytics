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

# Sources
cust_node = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_trusted", transformation_ctx="cust_node")
accel_node = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_landing", transformation_ctx="accel_node")

# Join and Distinct
cust_df = cust_node.toDF()
accel_df = accel_node.toDF()
joined_df = cust_df.join(accel_df, cust_df.email == accel_df.user, "inner")
curated_df = joined_df.select(cust_df["*"]).distinct()
curated_node = DynamicFrame.fromDF(curated_df, glueContext, "curated_node")

# Target
target_node = glueContext.getSink(path="s3://YOUR_BUCKET_NAME/customer/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="target_node")
target_node.setCatalogInfo(catalogDatabase="stedi", catalogTableName="customers_curated")
target_node.setFormat("glueparquet")
target_node.writeFrame(curated_node)
job.commit()