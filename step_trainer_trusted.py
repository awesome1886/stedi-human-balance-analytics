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

# --- Node 1: Source - Step Trainer Landing ---
step_trainer_node = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="step_trainer_landing", transformation_ctx="step_trainer_node")

# --- Node 2: Source - Customers Curated ---
customers_curated_node = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customers_curated", transformation_ctx="customers_curated_node")

# --- Node 3: Join ---
joined_node = Join.apply(frame1=step_trainer_node, frame2=customers_curated_node, keys1=["serialnumber"], keys2=["serialnumber"], transformation_ctx="joined_node")

# --- Node 4: Drop Fields ---
drop_fields_node = DropFields.apply(frame=joined_node, paths=["customername", "email", "phone", "birthday", "registrationdate", "lastupdatedate", "sharewithresearchasofdate", "sharewithpublicasofdate", "sharewithfriendsasofdate"], transformation_ctx="drop_fields_node")

# --- Node 5: Target - S3 Write ---
AmazonS3_node5 = glueContext.getSink(path="s3://johndoe-stedi-lakehouse-v3/step_trainer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node5")
AmazonS3_node5.setCatalogInfo(catalogDatabase="stedi", catalogTableName="step_trainer_trusted")
AmazonS3_node5.setFormat("glueparquet")
AmazonS3_node5.writeFrame(drop_fields_node)

job.commit()