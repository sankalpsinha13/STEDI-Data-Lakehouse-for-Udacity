import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node Accelerometer Landing
AccelerometerLanding_node1745314765868 = glueContext.create_dynamic_frame.from_catalog(database="sank_base", table_name="accelerometer_landing", transformation_ctx="AccelerometerLanding_node1745314765868")

# Script generated for node Customer Trusted
CustomerTrusted_node1745314768063 = glueContext.create_dynamic_frame.from_catalog(database="sank_base", table_name="customer_trusted", transformation_ctx="CustomerTrusted_node1745314768063")

# Script generated for node Join
Join_node1745314771765 = Join.apply(frame1=CustomerTrusted_node1745314768063, frame2=AccelerometerLanding_node1745314765868, keys1=["email"], keys2=["user"], transformation_ctx="Join_node1745314771765")

# Script generated for node Accelerometer Trusted
EvaluateDataQuality().process_rows(frame=Join_node1745314771765, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1745314759800", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AccelerometerTrusted_node1745314775107 = glueContext.getSink(path="s3://sank-bucket13/accelerometer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AccelerometerTrusted_node1745314775107")
AccelerometerTrusted_node1745314775107.setCatalogInfo(catalogDatabase="sank_base",catalogTableName="accelerometer_trusted")
AccelerometerTrusted_node1745314775107.setFormat("json")
AccelerometerTrusted_node1745314775107.writeFrame(Join_node1745314771765)
job.commit()