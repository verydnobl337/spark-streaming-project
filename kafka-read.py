from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, from_unixtime
from pyspark.sql.types import StructField, StringType, StructType, LongType

from config import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_INPUT_TOPIC,
    KAFKA_JAAS_CONFIG,
    KAFKA_PACKAGE,
    KAFKA_SASL_MECHANISM,
    KAFKA_SECURITY_PROTOCOL,
)

spark = (
    SparkSession.builder.appName("Kafka-read-test")
    .config("spark.jars.packages", KAFKA_PACKAGE)
    .getOrCreate()
)

kafka_df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
    .option("kafka.security.protocol", KAFKA_SECURITY_PROTOCOL)
    .option("kafka.sasl.jaas.config", KAFKA_JAAS_CONFIG)
    .option("kafka.sasl.mechanism", KAFKA_SASL_MECHANISM)
    .option("subscribe", KAFKA_INPUT_TOPIC)
    .option("startingOffsets", "earliest")
    .load()
)

incoming_message_schema = StructType(
    [
        StructField("restaurant_id", StringType(), True),
        StructField("adv_campaign_id", StringType(), True),
        StructField("adv_campaign_content", StringType(), True),
        StructField("adv_campaign_owner", StringType(), True),
        StructField("adv_campaign_owner_contact", StringType(), True),
        StructField("adv_campaign_datetime_start", LongType(), True),
        StructField("adv_campaign_datetime_end", LongType(), True),
        StructField("datetime_created", LongType(), True),
    ]
)

parsed_df = kafka_df.select(
    from_json(col("value").cast(StringType()), incoming_message_schema).alias(
        "parsed_value"
    )
)

result_df = parsed_df.select(
    col("parsed_value.restaurant_id"),
    col("parsed_value.adv_campaign_id"),
    col("parsed_value.adv_campaign_content"),
    col("parsed_value.adv_campaign_owner"),
    col("parsed_value.adv_campaign_owner_contact"),
    from_unixtime(col("parsed_value.adv_campaign_datetime_start"))
    .cast("timestamp")
    .alias("adv_campaign_datetime_start"),
    from_unixtime(col("parsed_value.adv_campaign_datetime_end"))
    .cast("timestamp")
    .alias("adv_campaign_datetime_end"),
    from_unixtime(col("parsed_value.datetime_created"))
    .cast("timestamp")
    .alias("datetime_created"),
)

(
    result_df.writeStream.format("console")
    .outputMode("append")
    .start()
    .awaitTermination()
)
