import sys
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

# Script generated for node Accelerometer Landing
AccelerometerLanding_node1698102418255 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="accelerometer_landing",
    transformation_ctx="AccelerometerLanding_node1698102418255",
)

# Script generated for node Customer Trusted
CustomerTrusted_node1698119001656 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="customer_trusted",
    transformation_ctx="CustomerTrusted_node1698119001656",
)

# Script generated for node Join by users
Joinbyusers_node1698120115222 = Join.apply(
    frame1=AccelerometerLanding_node1698102418255,
    frame2=CustomerTrusted_node1698119001656,
    keys1=["user"],
    keys2=["email"],
    transformation_ctx="Joinbyusers_node1698120115222",
)

# Script generated for node Drop Customer Fields
DropCustomerFields_node1698120152442 = DropFields.apply(
    frame=Joinbyusers_node1698120115222,
    paths=[
        "customername",
        "email",
        "phone",
        "birthday",
        "serialnumber",
        "registrationdate",
        "lastupdatedate",
        "sharewithresearchasofdate",
        "sharewithpublicasofdate",
    ],
    transformation_ctx="DropCustomerFields_node1698120152442",
)

# Script generated for node Amazon S3
AmazonS3_node1698102585564 = glueContext.getSink(
    path="s3://esron-lake-house/accelerometer/trusted/",
    connection_type="s3",
    updateBehavior="LOG",
    partitionKeys=[],
    compression="gzip",
    enableUpdateCatalog=True,
    transformation_ctx="AmazonS3_node1698102585564",
)
AmazonS3_node1698102585564.setCatalogInfo(
    catalogDatabase="stedi", catalogTableName="accelerometer_trusted"
)
AmazonS3_node1698102585564.setFormat("json")
AmazonS3_node1698102585564.writeFrame(DropCustomerFields_node1698120152442)
job.commit()
