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
accel_node = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_landing", transformation_ctx="accel_node")
cust_node = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_trusted", transformation_ctx="cust_node")

# Join
joined_node = Join.apply(frame1=accel_node, frame2=cust_node, keys1=["user"], keys2=["email"], transformation_ctx="joined_node")

# Drop Fields
drop_node = DropFields.apply(frame=joined_node, paths=["customername", "email", "phone", "birthday", "serialnumber", "registrationdate", "lastupdatedate", "sharewithresearchasofdate", "sharewithpublicasofdate", "sharewithfriendsasofdate"], transformation_ctx="drop_node")

# Target
target_node = glueContext.getSink(path="s3://YOUR_BUCKET_NAME/accelerometer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="target_node")
target_node.setCatalogInfo(catalogDatabase="stedi", catalogTableName="accelerometer_trusted")
target_node.setFormat("glueparquet")
target_node.writeFrame(drop_node)
job.commit()