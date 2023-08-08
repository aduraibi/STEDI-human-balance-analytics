import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import re

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Customer Landing Zone
CustomerLandingZone_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-human/customer/customer_landing/"],
        "recurse": True,
    },
    transformation_ctx="CustomerLandingZone_node1",
)

# Script generated for node Privacy filter
Privacyfilter_node1691480634830 = Filter.apply(
    frame=CustomerLandingZone_node1,
    f=lambda row: (not (row["shareWithResearchAsOfDate"] == 0)),
    transformation_ctx="Privacyfilter_node1691480634830",
)

# Script generated for node Customer Trusted Zone
CustomerTrustedZone_node3 = glueContext.write_dynamic_frame.from_options(
    frame=Privacyfilter_node1691480634830,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://stedi-human/customer/customer_trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="CustomerTrustedZone_node3",
)

job.commit()
