import xmlschema
from pyspark.sql.functions import *
from pyspark.sql.types import BooleanType
from pyspark.sql import SparkSession

@udf(returnType=BooleanType())
def xml_validation(file_name):
    schema_file = "/Users/kyarl1/spark_kafka/data/schema/sample_order.xsd"
    my_schema = xmlschema.XMLSchema(schema_file)
    return my_schema.is_valid(file_name)

source_file_location = '/Users/kyarl1/spark_kafka/data/files/'

spark = SparkSession.builder.appName("spark_src_to_kafka").getOrCreate()
df = spark.read.format("xml").option("rowTag","Root").load(source_file_location)

df1 = df.select("*", substring(input_file_name(),6,400).alias("fname"))

df2 = df1.select("*", xml_validation('fname').alias('status'))

final_df = df2.where(df2.status != False)


final_df.writeStream\
    .format("kafka")\
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)\
    .option("topic", KAFKA_TOPIC)\
    .option("checkpointLocation", "/tmp/checkpoint")\
    .trigger(once=True) \
    .start()\
    .awaitTermination()