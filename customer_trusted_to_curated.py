import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame
from pyspark.sql.functions import col

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# --- Node 1: Source - Customer Trusted ---
customer_trusted_node = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="customer_trusted",
    transformation_ctx="customer_trusted_node"
)

# --- Node 2: Source - Accelerometer Landing ---
accelerometer_landing_node = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="accelerometer_landing",
    transformation_ctx="accelerometer_landing_node"
)

# --- Node 3: Join and Filter ---
# We join to find customers who exist in BOTH tables.
# We convert to Spark DataFrame to handle the 'Distinct' operation easily.
customer_df = customer_trusted_node.toDF()
accelerometer_df = accelerometer_landing_node.toDF()

# Join on email == user
joined_df = customer_df.join(accelerometer_df, customer_df.email == accelerometer_df.user, "inner")

# Select only customer columns and remove duplicates (Distinct)
# We don't want a row for every single accelerometer reading, just one per customer.
curated_df = joined_df.select(customer_df["*"]).distinct()

# Convert back to DynamicFrame
curated_dynamic_frame = DynamicFrame.fromDF(curated_df, glueContext, "curated_dynamic_frame")

# --- Node 4: Target - S3 Write ---
AmazonS3_node4 = glueContext.getSink(
    path="s3://johndoe-stedi-lakehouse-v3/customer/curated/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="AmazonS3_node4"
)
AmazonS3_node4.setCatalogInfo(catalogDatabase="stedi", catalogTableName="customers_curated")
AmazonS3_node4.setFormat("glueparquet")
AmazonS3_node4.writeFrame(curated_dynamic_frame)

job.commit()