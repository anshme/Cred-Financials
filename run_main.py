# from pyspark.sql import SparkSession
# from pyhocon import ConfigFactory
#
# from kafka_consumer import get_df_from_kafka
# from happybase import Connection
#
#
# def get_spark_session(spark_conf):
#     application_name = spark_conf['application_name']
#     master = spark_conf['master']
#     log_level = spark_conf['log_level']
#     spark = (
#         SparkSession.builder.appName(application_name)
#         .master(master)
#         .getOrCreate()
#     )
#     spark.sparkContext.setLogLevel(log_level)
#
#     return spark, spark.sparkContext
#
#
# def get_happybase_connection(hbase_conf):
#     host = hbase_conf['host']
#     port = int(hbase_conf['port'])
#     hbase_connection = Connection(host=host, port=port)
#     return hbase_connection
#
#
# if __name__ == '__main__':
#     conf = ConfigFactory.parse_file('application.conf')
#
#     spark_conf = conf['spark']
#     spark, sc = get_spark_session(spark_conf=spark_conf)
#
#     kafka_conf = conf['kafka']
#     df = get_df_from_kafka(spark=spark, kafka_conf=kafka_conf)
#
#     hbase_conf = conf['hbase']
#     hbase_connection = get_happybase_connection(hbase_conf=hbase_conf)
#
#     sc.broadcast(hbase_connection)
