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

# Script generated for node Customer Landing
CustomerLanding_node1745299218068 = glueContext.create_dynamic_frame.from_catalog(database="sank_base", table_name="customer_landing", transformation_ctx="CustomerLanding_node1745299218068")

# Script generated for node Customer Landing to Trusted
SqlQuery749 = '''
select * from myDataSource
where sharewithresearchasofdate is not null
'''
CustomerLandingtoTrusted_node1745299249621 = sparkSqlQuery(glueContext, query = SqlQuery749, mapping = {"myDataSource":CustomerLanding_node1745299218068}, transformation_ctx = "CustomerLandingtoTrusted_node1745299249621")

# Script generated for node Customer Trusted
EvaluateDataQuality().process_rows(frame=CustomerLandingtoTrusted_node1745299249621, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1745298620803", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
CustomerTrusted_node1745299349009 = glueContext.getSink(path="s3://sank-bucket13/customer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="CustomerTrusted_node1745299349009")
CustomerTrusted_node1745299349009.setCatalogInfo(catalogDatabase="sank_base",catalogTableName="customer_trusted")
CustomerTrusted_node1745299349009.setFormat("json")
CustomerTrusted_node1745299349009.writeFrame(CustomerLandingtoTrusted_node1745299249621)
job.commit()