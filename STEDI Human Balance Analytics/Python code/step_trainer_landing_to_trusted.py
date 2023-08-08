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

# Script generated for node Step Trainer Landing
StepTrainerLanding_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-human/step_trainer/step_trainer_landing/"],
        "recurse": True,
    },
    transformation_ctx="StepTrainerLanding_node1",
)

# Script generated for node Customer Curated
CustomerCurated_node1691500664051 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-human/customer/customer_curated/"],
        "recurse": True,
    },
    transformation_ctx="CustomerCurated_node1691500664051",
)

# Script generated for node Join
Join_node1691500697750 = Join.apply(
    frame1=StepTrainerLanding_node1,
    frame2=CustomerCurated_node1691500664051,
    keys1=["serialNumber"],
    keys2=["serialNumber"],
    transformation_ctx="Join_node1691500697750",
)

# Script generated for node Drop Fields
DropFields_node1691500723878 = DropFields.apply(
    frame=Join_node1691500697750,
    paths=[
        "shareWithFriendsAsOfDate",
        "phone",
        "`.serialNumber`",
        "birthDay",
        "shareWithPublicAsOfDate",
        "shareWithResearchAsOfDate",
        "registrationDate",
        "customerName",
        "email",
        "lastUpdateDate",
    ],
    transformation_ctx="DropFields_node1691500723878",
)

# Script generated for node S3 bucket
S3bucket_node3 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1691500723878,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://stedi-human/step_trainer/step_trainer_trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="S3bucket_node3",
)

job.commit()
