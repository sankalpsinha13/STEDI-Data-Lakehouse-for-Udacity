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

# Script generated for node Customers Curated
CustomersCurated_node1745316307861 = glueContext.create_dynamic_frame.from_catalog(database="sank_base", table_name="customer_curated", transformation_ctx="CustomersCurated_node1745316307861")

# Script generated for node Step Trainer Landing
StepTrainerLanding_node1745316200491 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://sank-bucket13/step_trainer/landing/"], "recurse": True}, transformation_ctx="StepTrainerLanding_node1745316200491")

# Script generated for node Step Trainer Landing Updated
StepTrainerLandingUpdated_node1745316374489 = ApplyMapping.apply(frame=StepTrainerLanding_node1745316200491, mappings=[("sensorreadingtime", "long", "right_sensorreadingtime", "bigint"), ("serialnumber", "string", "right_serialnumber", "string"), ("distancefromobject", "int", "right_distancefromobject", "bigint")], transformation_ctx="StepTrainerLandingUpdated_node1745316374489")

# Script generated for node Join
Join_node1745316330982 = Join.apply(frame1=CustomersCurated_node1745316307861, frame2=StepTrainerLandingUpdated_node1745316374489, keys1=["serialnumber"], keys2=["right_serialnumber"], transformation_ctx="Join_node1745316330982")

# Script generated for node Step Trainer Trusted
EvaluateDataQuality().process_rows(frame=Join_node1745316330982, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1745316084890", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
StepTrainerTrusted_node1745316414102 = glueContext.getSink(path="s3://sank-bucket13/step_trainer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="StepTrainerTrusted_node1745316414102")
StepTrainerTrusted_node1745316414102.setCatalogInfo(catalogDatabase="sank_base",catalogTableName="step_trainer_trusted")
StepTrainerTrusted_node1745316414102.setFormat("json")
StepTrainerTrusted_node1745316414102.writeFrame(Join_node1745316330982)
job.commit()