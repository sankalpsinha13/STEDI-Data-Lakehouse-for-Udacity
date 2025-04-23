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
AccelerometerTrusted_node1745314765868 = glueContext.create_dynamic_frame.from_catalog(database="sank_base", table_name="accelerometer_trusted", transformation_ctx="AccelerometerTrusted_node1745314765868")

# Script generated for node Customer Trusted
CustomerTrusted_node1745314768063 = glueContext.create_dynamic_frame.from_catalog(database="sank_base", table_name="customer_trusted", transformation_ctx="CustomerTrusted_node1745314768063")

# Script generated for node Renamed keys for Join
RenamedkeysforJoin_node1745383468170 = ApplyMapping.apply(frame=AccelerometerTrusted_node1745314765868, mappings=[("serialnumber", "string", "right_serialnumber", "string"), ("z", "double", "right_z", "float"), ("birthday", "string", "right_birthday", "string"), ("sharewithpublicasofdate", "long", "right_sharewithpublicasofdate", "bigint"), ("sharewithresearchasofdate", "long", "right_sharewithresearchasofdate", "bigint"), ("registrationdate", "long", "right_registrationdate", "bigint"), ("customername", "string", "right_customername", "string"), ("user", "string", "right_user", "string"), ("y", "double", "right_y", "float"), ("sharewithfriendsasofdate", "long", "right_sharewithfriendsasofdate", "bigint"), ("x", "double", "right_x", "float"), ("timestamp", "long", "right_timestamp", "bigint"), ("email", "string", "right_email", "string"), ("lastupdatedate", "long", "right_lastupdatedate", "bigint"), ("phone", "string", "right_phone", "string")], transformation_ctx="RenamedkeysforJoin_node1745383468170")

# Script generated for node Join Tables
SqlQuery1252 = '''
select * from a
JOIN c 
ON a.right_user = c.email
'''
JoinTables_node1745385398134 = sparkSqlQuery(glueContext, query = SqlQuery1252, mapping = {"c":CustomerTrusted_node1745314768063, "a":RenamedkeysforJoin_node1745383468170}, transformation_ctx = "JoinTables_node1745385398134")

# Script generated for node Drop Fields and Duplicates
SqlQuery1251 = '''
select distinct customername, email, phone, birthday, 
serialnumber, registrationdate, lastupdatedate, 
sharewithresearchasofdate, sharewithpublicasofdate,
sharewithfriendsasofdate from myDataSource
'''
DropFieldsandDuplicates_node1745315552391 = sparkSqlQuery(glueContext, query = SqlQuery1251, mapping = {"myDataSource":JoinTables_node1745385398134}, transformation_ctx = "DropFieldsandDuplicates_node1745315552391")

# Script generated for node Customer Curated
EvaluateDataQuality().process_rows(frame=DropFieldsandDuplicates_node1745315552391, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1745314759800", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
CustomerCurated_node1745314775107 = glueContext.getSink(path="s3://sank-bucket13/customer/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="CustomerCurated_node1745314775107")
CustomerCurated_node1745314775107.setCatalogInfo(catalogDatabase="sank_base",catalogTableName="customer_curated")
CustomerCurated_node1745314775107.setFormat("json")
CustomerCurated_node1745314775107.writeFrame(DropFieldsandDuplicates_node1745315552391)
job.commit()