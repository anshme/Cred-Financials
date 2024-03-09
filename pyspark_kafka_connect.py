import json

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json
from pyspark.sql.types import StructType, StructField


def get_df_from_kafka(spark, kafka_conf=None):
    bootstrap_server = 'broker:29092'
    topic = "sampletopic"

    print("connecting to bootstrapserver " + bootstrap_server)
    print("topic subscribing " + topic)

    df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_server)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .load()
    )

    return df

def print_df_on_console(df):
    return df.writeStream.outputMode("append").format("console")

def get_df_from_schema(df, json_schema):
    return df.selectExpr("CAST(value AS STRING)").select(from_json("value", json_schema).alias("data")).select("data.*")

def read_schema(file_path):
    with open(file_path, "r") as f:
        schema_json = json.load(f)
    json_schema = StructType.fromJson(schema_json)
    return json_schema

application_name = "myApplication"
master = "local[*]"
log_level = "ERROR"
spark = (
    SparkSession.builder.appName(application_name)
    .master(master)
    .getOrCreate()
)
spark.sparkContext.setLogLevel(log_level)

kafka_df = get_df_from_kafka(spark=spark)

schema = read_schema('schema.json')
parsed_df = get_df_from_schema(kafka_df, schema)

query = print_df_on_console(parsed_df)

query.start().awaitTermination()