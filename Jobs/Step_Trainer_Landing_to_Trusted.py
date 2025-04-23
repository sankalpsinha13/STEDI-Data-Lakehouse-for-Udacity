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

# Script generated for node Step Trainer Landing
StepTrainerLanding_node1745385983610 = glueContext.create_dynamic_frame.from_catalog(database="sank_base", table_name="step_trainer_landing", transformation_ctx="StepTrainerLanding_node1745385983610")

# Script generated for node Customers Curated
CustomersCurated_node1745386041789 = glueContext.create_dynamic_frame.from_catalog(database="sank_base", table_name="customer_curated", transformation_ctx="CustomersCurated_node1745386041789")

# Script generated for node Step Trainer Landing Updated
StepTrainerLandingUpdated_node1745316374489 = ApplyMapping.apply(frame=StepTrainerLanding_node1745385983610, mappings=[("sensorreadingtime", "long", "right_sensorreadingtime", "bigint"), ("serialnumber", "string", "right_serialnumber", "string"), ("distancefromobject", "int", "right_distancefromobject", "bigint")], transformation_ctx="StepTrainerLandingUpdated_node1745316374489")

# Script generated for node Join Tables
SqlQuery1148 = '''
select * from s
JOIN c 
ON s.right_serialnumber = c.serialnumber
'''
JoinTables_node1745386120103 = sparkSqlQuery(glueContext, query = SqlQuery1148, mapping = {"c":CustomersCurated_node1745386041789, "s":StepTrainerLandingUpdated_node1745316374489}, transformation_ctx = "JoinTables_node1745386120103")

# Script generated for node Step Trainer Trusted
EvaluateDataQuality().process_rows(frame=JoinTables_node1745386120103, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1745316084890", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
StepTrainerTrusted_node1745316414102 = glueContext.getSink(path="s3://sank-bucket13/step_trainer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="StepTrainerTrusted_node1745316414102")
StepTrainerTrusted_node1745316414102.setCatalogInfo(catalogDatabase="sank_base",catalogTableName="step_trainer_trusted")
StepTrainerTrusted_node1745316414102.setFormat("json")
StepTrainerTrusted_node1745316414102.writeFrame(JoinTables_node1745386120103)
job.commit()