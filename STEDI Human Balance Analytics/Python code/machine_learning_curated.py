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

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-human/accelerometer/accelerometer_trusted/"],
        "recurse": True,
    },
    transformation_ctx="AccelerometerTrusted_node1",
)

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node1691500952137 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-human/step_trainer/step_trainer_trusted/"],
        "recurse": True,
    },
    transformation_ctx="StepTrainerTrusted_node1691500952137",
)

# Script generated for node Join
Join_node1691500955442 = Join.apply(
    frame1=StepTrainerTrusted_node1691500952137,
    frame2=AccelerometerTrusted_node1,
    keys1=["sensorReadingTime"],
    keys2=["timeStamp"],
    transformation_ctx="Join_node1691500955442",
)

# Script generated for node machine_learning_curated
machine_learning_curated_node3 = glueContext.write_dynamic_frame.from_options(
    frame=Join_node1691500955442,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://stedi-human/machine_learning_curated/",
        "partitionKeys": [],
    },
    transformation_ctx="machine_learning_curated_node3",
)

job.commit()
