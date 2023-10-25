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
AccelerometerTrusted_node1698189629878 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="accelerometer_trusted",
    transformation_ctx="AccelerometerTrusted_node1698189629878",
)

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node1698189629548 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="step_trainer_trusted",
    transformation_ctx="StepTrainerTrusted_node1698189629548",
)

# Script generated for node SQL Query
SqlQuery0 = """
select
    stt.sensorreadingtime,
    stt.serialnumber,
    stt.distancefromobject,
    at.x,
    at.y,
    at.z
from step_trainer_trusted as stt
join accelerometer_trusted as at
    on at.timestamp = stt.sensorreadingtime;
"""
SQLQuery_node1698189634338 = sparkSqlQuery(
    glueContext,
    query=SqlQuery0,
    mapping={
        "accelerometer_trusted": AccelerometerTrusted_node1698189629878,
        "step_trainer_trusted": StepTrainerTrusted_node1698189629548,
    },
    transformation_ctx="SQLQuery_node1698189634338",
)

# Script generated for node Machine Learning Curated
MachineLearningCurated_node1698189635540 = glueContext.getSink(
    path="s3://esron-lake-house/step_trainer/curated/",
    connection_type="s3",
    updateBehavior="LOG",
    partitionKeys=[],
    compression="gzip",
    enableUpdateCatalog=True,
    transformation_ctx="MachineLearningCurated_node1698189635540",
)
MachineLearningCurated_node1698189635540.setCatalogInfo(
    catalogDatabase="stedi", catalogTableName="machine_learning_curated"
)
MachineLearningCurated_node1698189635540.setFormat("json")
MachineLearningCurated_node1698189635540.writeFrame(SQLQuery_node1698189634338)
job.commit()
