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

# Script generated for node Customer Curated
CustomerCurated_node1698188867550 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="customer_curated",
    transformation_ctx="CustomerCurated_node1698188867550",
)

# Script generated for node Step Trainer Landing
StepTrainerLanding_node1698188814384 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="step_trainer_landing",
    transformation_ctx="StepTrainerLanding_node1698188814384",
)

# Script generated for node Share with research
SqlQuery0 = """
select
    stl.sensorreadingtime,
    stl.serialnumber,
    stl.distancefromobject
from step_trainer_landing as stl
join customer_curated as cc
on cc.serialnumber = stl.serialnumber
"""
Sharewithresearch_node1698188898081 = sparkSqlQuery(
    glueContext,
    query=SqlQuery0,
    mapping={
        "customer_curated": CustomerCurated_node1698188867550,
        "step_trainer_landing": StepTrainerLanding_node1698188814384,
    },
    transformation_ctx="Sharewithresearch_node1698188898081",
)

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node1698189078544 = glueContext.getSink(
    path="s3://esron-lake-house/step_trainer/trusted/",
    connection_type="s3",
    updateBehavior="LOG",
    partitionKeys=[],
    compression="gzip",
    enableUpdateCatalog=True,
    transformation_ctx="StepTrainerTrusted_node1698189078544",
)
StepTrainerTrusted_node1698189078544.setCatalogInfo(
    catalogDatabase="stedi", catalogTableName="step_trainer_trusted"
)
StepTrainerTrusted_node1698189078544.setFormat("json")
StepTrainerTrusted_node1698189078544.writeFrame(Sharewithresearch_node1698188898081)
job.commit()
