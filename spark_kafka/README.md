This project has 3 scripts under src/streaming folder.
1. spark_src_to_kafka.py is to read xml files from source and write to kafka streaming platform
2. spark_kafka_to_raw.py is the read data from kafka topic and write to raw layer in parquet format, the data is partitioned by load_date_time.
3. spark_raw_to_processed.py is to read data from raw layer and write to processed layer.


we can include separate scripts to archive data from raw layer to designated location on a periodic basis.


