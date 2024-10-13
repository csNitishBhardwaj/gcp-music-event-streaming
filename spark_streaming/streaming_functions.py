from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, month, hour, dayofmonth, col, year, udf

@udf
def string_decode(s, encoding='utf-8'):
    if s:
        return s.encode('latin1') \
                .decode('unicode-escape') \
                .encode('latin1') \
                .decode(encoding) \
                .strip('\"')
    else:
        return s

def create_or_get_spark_session(app_name, master="yarn"):
    spark = SparkSession.builder \
                .appName(app_name) \
                .master(master=master) \
                .getOrCreate()
    
    return spark

def create_kafka_read_stream(spark, kafka_address, kafka_port, topic, starting_offset="earliest"):
    read_stream = spark.readStream \
                    .format("kafka") \
                    .option("kafka.bootstrap.servers", f"{kafka_address}:{kafka_port}") \
                    .option("failOnDataLoss", False) \
                    .option("startingOffsets", starting_offset) \
                    .option("subscribe", topic) \
                    .load()
    
    return read_stream

def process_stream(stream, stream_schema, topic):
    stream = stream \
                .selectExpr("CAST(value AS STRING)") \
                .select(from_json("value"), stream_schema) \
                .alias("data") \
                .select("data.*")

    # Add month, day, hour to split data into seperate directories      
    stream = stream \
                .withColumn("ts", (col("ts")/1000).cast("timestamp")) \
                .withColumn("year", year(col("ts"))) \
                .withColumn("month", month(col("ts"))) \
                .withColumn("hour", hour(col("ts"))) \
                .withColumn("day", dayofmonth(col("ts")))
    
    # Rectify string encoding
    if topic in ["listen_events", "page_view_events"]:
        stream = stream \
                    .withColumn("song", string_decode("song")) \
                    .withColumn("artist", string_decode("artist"))
    
    return stream
        
def create_file_write_stream(stream, storage_path, checkpoint_path, trigger="120 seconds", output_mode="append", file_format="parquet"):
    write_stream = stream \
                    .writeStream \
                    .format(file_format) \
                    .partitionBy("month", "day", "hour") \
                    .option("path", storage_path) \
                    .option("checkpointLocation", checkpoint_path) \
                    .trigger(processingTime=trigger) \
                    .outputMode(output_mode)
    
    return write_stream