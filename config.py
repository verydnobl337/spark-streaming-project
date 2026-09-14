import os


def required_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Environment variable {name} is required")
    return value


KAFKA_BOOTSTRAP_SERVERS = required_env("KAFKA_BOOTSTRAP_SERVERS")
KAFKA_SECURITY_PROTOCOL = os.getenv("KAFKA_SECURITY_PROTOCOL", "SASL_SSL")
KAFKA_SASL_MECHANISM = os.getenv("KAFKA_SASL_MECHANISM", "SCRAM-SHA-512")
KAFKA_USERNAME = required_env("KAFKA_USERNAME")
KAFKA_PASSWORD = required_env("KAFKA_PASSWORD")
KAFKA_INPUT_TOPIC = required_env("KAFKA_INPUT_TOPIC")
KAFKA_OUTPUT_TOPIC = required_env("KAFKA_OUTPUT_TOPIC")

POSTGRES_SOURCE_URL = required_env("POSTGRES_SOURCE_URL")
POSTGRES_SOURCE_USER = required_env("POSTGRES_SOURCE_USER")
POSTGRES_SOURCE_PASSWORD = required_env("POSTGRES_SOURCE_PASSWORD")
POSTGRES_SOURCE_TABLE = os.getenv("POSTGRES_SOURCE_TABLE", "subscribers_restaurants")

POSTGRES_TARGET_URL = required_env("POSTGRES_TARGET_URL")
POSTGRES_TARGET_USER = required_env("POSTGRES_TARGET_USER")
POSTGRES_TARGET_PASSWORD = required_env("POSTGRES_TARGET_PASSWORD")
POSTGRES_TARGET_TABLE = os.getenv(
    "POSTGRES_TARGET_TABLE", "subscribers_feedback_s27040058"
)

KAFKA_JAAS_CONFIG = (
    "org.apache.kafka.common.security.scram.ScramLoginModule required "
    f'username="{KAFKA_USERNAME}" password="{KAFKA_PASSWORD}";'
)

KAFKA_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0"
POSTGRES_PACKAGE = "org.postgresql:postgresql:42.4.0"
