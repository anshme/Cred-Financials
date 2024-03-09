from datetime import datetime
import json
import sys

from confluent_kafka import KafkaError, KafkaException
from pyhocon import ConfigFactory

import dao
from geo_map import GEO_Map
from kafka_consumer import kafka_consumer

speed_threshold = 0.25
score_threshold = 200


def get_time_difference(timestamp1, timestamp2):
    format = "%d-%m-%Y %H:%M:%S"
    datetime1 = datetime.strptime(timestamp1, format)
    datetime2 = datetime.strptime(timestamp2, format)
    difference = datetime2 - datetime1
    return difference


def get_distance(geo, src_postcode, dest_postcode):
    source_lat = geo.get_lat(src_postcode)
    source_long = geo.get_lat(src_postcode)
    destination_lat = geo.get_lat(dest_postcode)
    destination_long = geo.get_long(dest_postcode)
    distance = geo.distance(source_lat, source_long, destination_lat, destination_long)
    return distance


def get_details_from_last_txn(hbase_connection, card_id, table_name):
    row = hbase_connection.get_data(key=str.encode(card_id), table=table_name)
    return row.get(b'st:pc'), row.get(b'bt:score'), row.get(b'bt:ucl'), row.get(b'st:tdt')


def psuh_to_hbase():
    pass


def check_if_fraud():
    pass


def execute():
    hbase_connection = dao.HBaseDao()
    geo = GEO_Map()
    conf = ConfigFactory.parse_file('application.conf')

    consumer = kafka_consumer(conf['kafka'])
    for msg in consumer:
        incoming_msg = json.loads(msg.value)
        last_postcode, credit_score, txn_time, ucl = get_details_from_last_txn(hbase_connection, str(incoming_msg['card_id']),"lookup")
        incoming_msg['last_postcode'] = last_postcode
        incoming_msg['credit_score'] = int(credit_score)
        incoming_msg['last_txn_time'] = txn_time
        incoming_msg['ucl'] = ucl
        incoming_msg['distance'] = get_distance(geo, incoming_msg['last_postcode'], incoming_msg['postcode'])
        incoming_msg['time_diff'] = get_time_difference(incoming_msg['last_txn_time'],
                                                        incoming_msg['transaction_dt'])
        print(incoming_msg)


if __name__ == '__main__':
    execute()
    # hbase_connection = dao.HBaseDao()
    # print(get_details_from_last_txn(hbase_connection, '348684315090900', 'lookup'))
