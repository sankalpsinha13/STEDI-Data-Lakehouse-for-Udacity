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

# Script generated for node Step Trainer Landing
StepTrainerLanding_node1745316200491 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://sank-bucket13/step_trainer/landing/"], "recurse": True}, transformation_ctx="StepTrainerLanding_node1745316200491")

# Script generated for node Step Trainer Landing Updated
StepTrainerLandingUpdated_node1745316374489 = ApplyMapping.apply(frame=StepTrainerLanding_node1745316200491, mappings=[("sensorreadingtime", "long", "right_sensorreadingtime", "bigint"), ("serialnumber", "string", "right_serialnumber", "string"), ("distancefromobject", "int", "right_distancefromobject", "bigint")], transformation_ctx="StepTrainerLandingUpdated_node1745316374489")

# Script generated for node Join
Join_node1745316330982 = Join.apply(frame1=AccelerometerTrusted_node1745316307861, frame2=StepTrainerLandingUpdated_node1745316374489, keys1=["timestamp"], keys2=["right_sensorreadingtime"], transformation_ctx="Join_node1745316330982")

# Script generated for node Modify Fields
SqlQuery672 = '''
select right_sensorreadingtime as sensorreadingtime, 
right_serialnumber as serialnumber, 
right_distancefromobject as distancefromobject, user, x, y, z
from myDataSource

'''
ModifyFields_node1745316979153 = sparkSqlQuery(glueContext, query = SqlQuery672, mapping = {"myDataSource":Join_node1745316330982}, transformation_ctx = "ModifyFields_node1745316979153")

# Script generated for node Machine Learning Curated
EvaluateDataQuality().process_rows(frame=ModifyFields_node1745316979153, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1745316084890", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
MachineLearningCurated_node1745316414102 = glueContext.getSink(path="s3://sank-bucket13/step_trainer/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="MachineLearningCurated_node1745316414102")
MachineLearningCurated_node1745316414102.setCatalogInfo(catalogDatabase="sank_base",catalogTableName="machine_learning_curated")
MachineLearningCurated_node1745316414102.setFormat("json")
MachineLearningCurated_node1745316414102.writeFrame(ModifyFields_node1745316979153)
job.commit()