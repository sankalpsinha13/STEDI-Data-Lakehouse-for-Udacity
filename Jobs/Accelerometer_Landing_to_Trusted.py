import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue import DynamicFrame

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
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

# Script generated for node Customer Trusted
CustomerTrusted_node1745314768063 = glueContext.create_dynamic_frame.from_catalog(database="sank_base", table_name="customer_trusted", transformation_ctx="CustomerTrusted_node1745314768063")

# Script generated for node Accelerometer Landing
AccelerometerLanding_node1745386657068 = glueContext.create_dynamic_frame.from_catalog(database="sank_base", table_name="accelerometer_landing", transformation_ctx="AccelerometerLanding_node1745386657068")

# Script generated for node Join Tables
SqlQuery1221 = '''
select * from a
JOIN c 
ON a.user = c.email
'''
JoinTables_node1745386487270 = sparkSqlQuery(glueContext, query = SqlQuery1221, mapping = {"c":CustomerTrusted_node1745314768063, "a":AccelerometerLanding_node1745386657068}, transformation_ctx = "JoinTables_node1745386487270")

# Script generated for node Accelerometer Trusted
EvaluateDataQuality().process_rows(frame=JoinTables_node1745386487270, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1745314759800", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AccelerometerTrusted_node1745314775107 = glueContext.getSink(path="s3://sank-bucket13/accelerometer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AccelerometerTrusted_node1745314775107")
AccelerometerTrusted_node1745314775107.setCatalogInfo(catalogDatabase="sank_base",catalogTableName="accelerometer_trusted")
AccelerometerTrusted_node1745314775107.setFormat("json")
AccelerometerTrusted_node1745314775107.writeFrame(JoinTables_node1745386487270)
job.commit()