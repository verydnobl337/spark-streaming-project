from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    current_timestamp,
    from_json,
    from_unixtime,
    lit,
    unix_timestamp,
)
from pyspark.sql.types import LongType, StringType, StructField, StructType

from config import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_INPUT_TOPIC,
    KAFKA_JAAS_CONFIG,
    KAFKA_PACKAGE,
    KAFKA_SASL_MECHANISM,
    KAFKA_SECURITY_PROTOCOL,
    POSTGRES_PACKAGE,
    POSTGRES_SOURCE_PASSWORD,
    POSTGRES_SOURCE_TABLE,
    POSTGRES_SOURCE_URL,
    POSTGRES_SOURCE_USER,
)

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
    .filter(
        (col("adv_campaign_datetime_start") <= current_timestamp())
        & (col("adv_campaign_datetime_end") >= current_timestamp())
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
    filtered_read_stream_df.join(subscribers_restaurant_df, "restaurant_id", "inner")
    .withColumn("trigger_datetime_created", unix_timestamp(current_timestamp()))
    .withColumn("feedback", lit(None).cast(StringType()))
)

(
    result_df.writeStream.format("console")
    .outputMode("append")
    .option("truncate", False)
    .start()
    .awaitTermination()
)
