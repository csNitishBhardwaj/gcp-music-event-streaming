import os
from streaming_functions import *
from schema import schema

# Kafka Topics
LISTEN_EVENTS_TOPIC = "listen_events"
PAGE_VIEW_EVENTS_TOPIC = "page_view_events"
AUTH_EVENTS_TOPIC = "auth_events"

KAFKA_PORT = "9092"

KAFKA_ADDRESS = os.getenv("KAFKA_ADDRESS", 'localhost')
GCP_GCS_BUCKET = os.getenv("GCP_GCS_BUCKET")
GCS_STORAGE_PATH = F'gs://{GCP_GCS_BUCKET}'

spark = create_or_get_spark_session('Eventsim-stream-v1')
spark.streams.resetTerminated()

# listen events stream
listen_events = create_kafka_read_stream(spark, KAFKA_ADDRESS, KAFKA_PORT, LISTEN_EVENTS_TOPIC)
listen_events = process_stream(listen_events, schema[LISTEN_EVENTS_TOPIC], LISTEN_EVENTS_TOPIC)

# page view stream
page_view_events = create_kafka_read_stream(spark, KAFKA_ADDRESS, KAFKA_PORT, PAGE_VIEW_EVENTS_TOPIC)
page_view_events = process_stream(page_view_events, schema[PAGE_VIEW_EVENTS_TOPIC], PAGE_VIEW_EVENTS_TOPIC)

# auth stream
auth_events = create_kafka_read_stream(spark, KAFKA_ADDRESS, KAFKA_PORT, AUTH_EVENTS_TOPIC)
auth_events = process_stream(auth_events, schema[AUTH_EVENTS_TOPIC], AUTH_EVENTS_TOPIC)

# write to sink -> GCS every 2 minutes in parquet
listen_events_writer = create_file_write_stream(listen_events,
                                                f"{GCS_STORAGE_PATH}/{LISTEN_EVENTS_TOPIC}",
                                                f"{GCS_STORAGE_PATH}/checkpoint{LISTEN_EVENTS_TOPIC}"
                                                )

page_view_events_writer = create_file_write_stream(page_view_events,
                                                f"{GCS_STORAGE_PATH}/{PAGE_VIEW_EVENTS_TOPIC}",
                                                f"{GCS_STORAGE_PATH}/checkpoint{PAGE_VIEW_EVENTS_TOPIC}"
                                                )


auth_events_writer = create_file_write_stream(auth_events,
                                                f"{GCS_STORAGE_PATH}/{AUTH_EVENTS_TOPIC}",
                                                f"{GCS_STORAGE_PATH}/checkpoint{AUTH_EVENTS_TOPIC}"
                                                )

listen_events_writer.start()
page_view_events_writer.start()
auth_events_writer.start()

spark.streams.awaitAnyTermination()