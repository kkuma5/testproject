from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, date_trunc


KAFKA_BOOTSTRAP_SERVERS = "kafka:1111"
KAFKA_TOPIC = "raw_xml"

spark = SparkSession.builder.appName("kafka_to_raw").getOrCreate()

df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
    .option("subscribe", KAFKA_TOPIC) \
    .option("startingOffsets", "earliest") \
    .load()

checkpoint_location = "/tmp/checkpoint"
trigger_interval = "180 seconds"

df_final = df.withColumn("load_date_time", date_trunc("Hour",current_timestamp()))

df_final.writeStream \
    .partitionBy(load_date_time) \
    .outputMode("complete") \
    .format("parquet") \
    .option("checkpointLocation", checkpoint_location) \
    .option("path", "target_dir") \
    .trigger(processingTime=trigger_interval) \
    .start() \
    .awaitTermination()