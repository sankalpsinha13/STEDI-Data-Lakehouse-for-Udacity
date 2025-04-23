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

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1745316307861 = glueContext.create_dynamic_frame.from_catalog(database="sank_base", table_name="accelerometer_trusted", transformation_ctx="AccelerometerTrusted_node1745316307861")

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node1745384724941 = glueContext.create_dynamic_frame.from_catalog(database="sank_base", table_name="step_trainer_trusted", transformation_ctx="StepTrainerTrusted_node1745384724941")

# Script generated for node Join Tables
SqlQuery1224 = '''
select * from a
JOIN s 
ON a.timestamp = s.right_sensorreadingtime
'''
JoinTables_node1745384717558 = sparkSqlQuery(glueContext, query = SqlQuery1224, mapping = {"a":AccelerometerTrusted_node1745316307861, "s":StepTrainerTrusted_node1745384724941}, transformation_ctx = "JoinTables_node1745384717558")

# Script generated for node Modify Fields
SqlQuery1223 = '''
select right_sensorreadingtime as sensorreadingtime, 
right_serialnumber as serialnumber, 
right_distancefromobject as distancefromobject, user, x, y, z
from myDataSource
'''
ModifyFields_node1745316979153 = sparkSqlQuery(glueContext, query = SqlQuery1223, mapping = {"myDataSource":JoinTables_node1745384717558}, transformation_ctx = "ModifyFields_node1745316979153")

# Script generated for node Machine Learning Curated
EvaluateDataQuality().process_rows(frame=ModifyFields_node1745316979153, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1745316084890", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
MachineLearningCurated_node1745316414102 = glueContext.getSink(path="s3://sank-bucket13/step_trainer/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="MachineLearningCurated_node1745316414102")
MachineLearningCurated_node1745316414102.setCatalogInfo(catalogDatabase="sank_base",catalogTableName="machine_learning_curated")
MachineLearningCurated_node1745316414102.setFormat("json")
MachineLearningCurated_node1745316414102.writeFrame(ModifyFields_node1745316979153)
job.commit()