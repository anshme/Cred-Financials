import json

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StructType, StructField, StringType
import dao
from happybase import Connection

from geo_map import GEO_Map
from kafka_consumer import get_df_from_kafka, get_df_from_schema, print_df_on_console, read_schema

hbase_connection = dao.HBaseDao()

def get_spark_session(spark_conf=None):
    application_name = "myApplication"
    master = "local[*]"
    log_level = "ERROR"
    spark = (
        SparkSession.builder.appName(application_name)
        .master(master)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel(log_level)

    return spark, spark.sparkContext

def get_last_source(value, ):
    rows = hbase_connection.get_data(key=str.encode(value), table='lookup')
    return rows['pc']

def get_credit_scroe(value):
    rows = hbase_connection.get_data(key=str.encode(value), table='lookup')
    return rows['score']

def get_distance(src_postcode, dest_postcode):
    geo = GEO_Map()
    source_lat = geo.get_lat(src_postcode)
    source_long = geo.get_lat(src_postcode)
    destination_lat = geo.get_lat(dest_postcode)
    destination_long = geo.get_long(dest_postcode)
    distance = geo.distance(source_lat, source_long, destination_lat, destination_long)
    return distance

if __name__ == '__main__':
    # conf = ConfigFactory.parse_file('application.conf')

    # spark_conf = conf['spark']
    spark, sc = get_spark_session()

    hbase_connection = dao.HBaseDao()

    print("Created spark session")

    # kafka_conf = conf['kafka']
    kafka_df = get_df_from_kafka(spark=spark)

    sc.broadcast(hbase_connection)
    print("connected to kafka")

    schema = StructType([
        StructField("card_id", StringType(), nullable=False),
        StructField("member_id", StringType(), nullable=False),
        StructField("amount", StringType(), nullable=False),
        StructField("postcode", StringType(), nullable=False),
        StructField("pos_id", StringType(), nullable=False),
        StructField("transaction_dt", StringType(), nullable=False)
    ])

    parsed_df = get_df_from_schema(kafka_df, schema)

    udf_last_postcode = udf(get_last_source, StringType())
    udf_credit_score = udf(get_credit_scroe, StringType())

    last_txn_pos_df = parsed_df.withColumn("last_postcode", udf_last_postcode('card_id'))
    credit_df = last_txn_pos_df.withColumn("credit_score", udf_last_postcode('card_id'))

    credit_df.writeStream.outputMode("append").format("console").start().awaitTermination()

    # hbase_connection = dao.HBaseDao()
    #
    # rows = hbase_connection.get_data(key=b'6544649161377464', table='lookup')
    # print(rows)

