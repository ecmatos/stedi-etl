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

# Script generated for node Customer Landing
CustomerLanding_node1698115716970 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://esron-lake-house/customer/landing/"],
        "recurse": True,
    },
    transformation_ctx="CustomerLanding_node1698115716970",
)

# Script generated for node Share with research
SqlQuery0 = """
select * 
from customer_landing where shareWithResearchAsOfDate <> 0;

"""
Sharewithresearch_node1698116801632 = sparkSqlQuery(
    glueContext,
    query=SqlQuery0,
    mapping={"customer_landing": CustomerLanding_node1698115716970},
    transformation_ctx="Sharewithresearch_node1698116801632",
)

# Script generated for node Amazon S3
AmazonS3_node1698115836087 = glueContext.getSink(
    path="s3://esron-lake-house/customer/trusted/",
    connection_type="s3",
    updateBehavior="LOG",
    partitionKeys=[],
    compression="gzip",
    enableUpdateCatalog=True,
    transformation_ctx="AmazonS3_node1698115836087",
)
AmazonS3_node1698115836087.setCatalogInfo(
    catalogDatabase="stedi", catalogTableName="customer_trusted"
)
AmazonS3_node1698115836087.setFormat("json")
AmazonS3_node1698115836087.writeFrame(Sharewithresearch_node1698116801632)
job.commit()
