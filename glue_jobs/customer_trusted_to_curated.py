import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame


def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)


args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1698102418255 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="accelerometer_trusted",
    transformation_ctx="AccelerometerTrusted_node1698102418255",
)

# Script generated for node Customer Trusted
CustomerTrusted_node1698119001656 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="customer_trusted",
    transformation_ctx="CustomerTrusted_node1698119001656",
)

# Script generated for node Step Trainer Landing
StepTrainerLanding_node1698185363342 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="step_trainer_landing",
    transformation_ctx="StepTrainerLanding_node1698185363342",
)

# Script generated for node Curated Customers
SqlQuery0 = """
select distinct 
 ct.customername,
 ct.email,
 ct.phone,
 ct.birthday,
 stl.serialnumber,
 ct.registrationdate,
 ct.lastupdatedate,
 ct.sharewithresearchasofdate,
 ct.sharewithpublicasofdate
from customer_trusted as ct
join accelerometer_trusted as at
    on at.user = ct.email
join step_trainer_landing as stl
    on stl.sensorreadingtime = at.timestamp
    and stl.serialnumber = ct.serialnumber;
"""
CuratedCustomers_node1698185440154 = sparkSqlQuery(
    glueContext,
    query=SqlQuery0,
    mapping={
        "accelerometer_trusted": AccelerometerTrusted_node1698102418255,
        "step_trainer_landing": StepTrainerLanding_node1698185363342,
        "customer_trusted": CustomerTrusted_node1698119001656,
    },
    transformation_ctx="CuratedCustomers_node1698185440154",
)

# Script generated for node Amazon S3
AmazonS3_node1698102585564 = glueContext.getSink(
    path="s3://esron-lake-house/customer/curated/",
    connection_type="s3",
    updateBehavior="LOG",
    partitionKeys=[],
    compression="gzip",
    enableUpdateCatalog=True,
    transformation_ctx="AmazonS3_node1698102585564",
)
AmazonS3_node1698102585564.setCatalogInfo(
    catalogDatabase="stedi", catalogTableName="customer_curated"
)
AmazonS3_node1698102585564.setFormat("json")
AmazonS3_node1698102585564.writeFrame(CuratedCustomers_node1698185440154)
job.commit()
