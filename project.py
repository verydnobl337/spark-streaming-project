from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, from_json, lit, struct, to_json, unix_timestamp
from pyspark.sql.types import LongType, StringType, StructField, StructType

from config import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_INPUT_TOPIC,
    KAFKA_JAAS_CONFIG,
    KAFKA_OUTPUT_TOPIC,
    KAFKA_PACKAGE,
    KAFKA_SASL_MECHANISM,
    KAFKA_SECURITY_PROTOCOL,
    POSTGRES_PACKAGE,
    POSTGRES_SOURCE_PASSWORD,
    POSTGRES_SOURCE_TABLE,
    POSTGRES_SOURCE_URL,
    POSTGRES_SOURCE_USER,
    POSTGRES_TARGET_PASSWORD,
    POSTGRES_TARGET_TABLE,
    POSTGRES_TARGET_URL,
    POSTGRES_TARGET_USER,
)


def foreach_batch_function(df, epoch_id):
    df.persist()

    try:
        (
            df.write.format("jdbc")
            .option("url", POSTGRES_TARGET_URL)
            .option("driver", "org.postgresql.Driver")
            .option("dbtable", POSTGRES_TARGET_TABLE)
            .option("user", POSTGRES_TARGET_USER)
            .option("password", POSTGRES_TARGET_PASSWORD)
            .mode("append")
            .save()
        )

        kafka_df = df.drop("feedback").select(to_json(struct("*")).alias("value"))

        (
            kafka_df.write.format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
            .option("kafka.security.protocol", KAFKA_SECURITY_PROTOCOL)
            .option("kafka.sasl.jaas.config", KAFKA_JAAS_CONFIG)
            .option("kafka.sasl.mechanism", KAFKA_SASL_MECHANISM)
            .option("topic", KAFKA_OUTPUT_TOPIC)
            .save()
        )
    finally:
        df.unpersist()


spark = (
    SparkSession.builder.appName("RestaurantSubscribeStreamingService")
    .config("spark.sql.session.timeZone", "UTC")
    .config("spark.jars.packages", f"{KAFKA_PACKAGE},{POSTGRES_PACKAGE}")
    .getOrCreate()
)

restaurant_read_stream_df = (
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

current_unix_timestamp = unix_timestamp(current_timestamp())

filtered_read_stream_df = (
    restaurant_read_stream_df.select(
        from_json(col("value").cast(StringType()), incoming_message_schema).alias(
            "parsed_value"
        )
    )
    .select(
        col("parsed_value.restaurant_id"),
        col("parsed_value.adv_campaign_id"),
        col("parsed_value.adv_campaign_content"),
        col("parsed_value.adv_campaign_owner"),
        col("parsed_value.adv_campaign_owner_contact"),
        col("parsed_value.adv_campaign_datetime_start"),
        col("parsed_value.adv_campaign_datetime_end"),
        col("parsed_value.datetime_created"),
    )
    .filter(
        (col("adv_campaign_datetime_start") <= current_unix_timestamp)
        & (col("adv_campaign_datetime_end") >= current_unix_timestamp)
    )
)

subscribers_restaurant_df = (
    spark.read.format("jdbc")
    .option("url", POSTGRES_SOURCE_URL)
    .option("driver", "org.postgresql.Driver")
    .option("dbtable", POSTGRES_SOURCE_TABLE)
    .option("user", POSTGRES_SOURCE_USER)
    .option("password", POSTGRES_SOURCE_PASSWORD)
    .load()
    .select("client_id", "restaurant_id")
)

result_df = (
    filtered_read_stream_df.join(
        subscribers_restaurant_df, "restaurant_id", "inner"
    )
    .withColumn("trigger_datetime_created", unix_timestamp(current_timestamp()))
    .withColumn("feedback", lit(None).cast(StringType()))
)

result_df.writeStream.foreachBatch(foreach_batch_function).start().awaitTermination()
