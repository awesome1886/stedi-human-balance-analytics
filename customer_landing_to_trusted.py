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

# --- Node 1: Read from Data Catalog ---
customer_landing_node = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="customer_landing",
    transformation_ctx="customer_landing_node"
)

# --- Node 2: Filter using Python ---
def filter_consenting_customers(rec):
    if "sharewithresearchasofdate" in rec and rec["sharewithresearchasofdate"] is not None:
        return True
    if "shareWithResearchAsOfDate" in rec and rec["shareWithResearchAsOfDate"] is not None:
        return True
    return False

filtered_customers = Filter.apply(frame=customer_landing_node, f=filter_consenting_customers)

# --- Node 3: Write to S3 ---
AmazonS3_node3 = glueContext.getSink(
    path="s3://johndoe-stedi-lakehouse-v3/customer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="AmazonS3_node3"
)
AmazonS3_node3.setCatalogInfo(catalogDatabase="stedi", catalogTableName="customer_trusted")
AmazonS3_node3.setFormat("glueparquet")
AmazonS3_node3.writeFrame(filtered_customers)

job.commit()
```

---

### **2. Update `accelerometer_landing_to_trusted.py`**

Open your local file `accelerometer_landing_to_trusted.py`, clear it, and paste this code:

```python
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

# --- Node 1: Source - Accelerometer Landing ---
accelerometer_landing_node = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="accelerometer_landing",
    transformation_ctx="accelerometer_landing_node"
)

# --- Node 2: Source - Customer Trusted ---
customer_trusted_node = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="customer_trusted",
    transformation_ctx="customer_trusted_node"
)

# --- Node 3: Join ---
joined_node = Join.apply(
    frame1=accelerometer_landing_node,
    frame2=customer_trusted_node,
    keys1=["user"],
    keys2=["email"],
    transformation_ctx="joined_node"
)

# --- Node 4: Drop Fields ---
drop_fields_node = DropFields.apply(
    frame=joined_node,
    paths=["customername", "email", "phone", "birthday", "serialnumber", 
           "registrationdate", "lastupdatedate", "sharewithresearchasofdate", 
           "sharewithpublicasofdate", "sharewithfriendsasofdate"],
    transformation_ctx="drop_fields_node"
)

# --- Node 5: Target - S3 Write ---
AmazonS3_node5 = glueContext.getSink(
    path="s3://johndoe-stedi-lakehouse-v3/accelerometer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="AmazonS3_node5"
)
AmazonS3_node5.setCatalogInfo(catalogDatabase="stedi", catalogTableName="accelerometer_trusted")
AmazonS3_node5.setFormat("glueparquet")
AmazonS3_node5.writeFrame(drop_fields_node)

job.commit()