from pyspark.sql import SparkSession

from config import (
    POSTGRES_PACKAGE,
    POSTGRES_SOURCE_PASSWORD,
    POSTGRES_SOURCE_TABLE,
    POSTGRES_SOURCE_URL,
    POSTGRES_SOURCE_USER,
)

spark = (
    SparkSession.builder.appName("Postgres-read-test")
    .config("spark.jars.packages", POSTGRES_PACKAGE)
    .getOrCreate()
)

subscribers_restaurant_df = (
    spark.read.format("jdbc")
    .option("url", POSTGRES_SOURCE_URL)
    .option("driver", "org.postgresql.Driver")
    .option("dbtable", POSTGRES_SOURCE_TABLE)
    .option("user", POSTGRES_SOURCE_USER)
    .option("password", POSTGRES_SOURCE_PASSWORD)
    .load()
)

subscribers_restaurant_df.show()
