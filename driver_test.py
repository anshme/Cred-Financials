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
    format1 = "%d-%m-%Y %H:%M:%S"
    datetime1 = datetime.strptime(timestamp1, format1)
    datetime2 = datetime.strptime(timestamp2, format1)
    difference = datetime2 - datetime1
    time_difference_seconds = difference.total_seconds()
    return time_difference_seconds


def get_distance(geo, src_postcode, dest_postcode):
    source_lat = geo.get_lat(src_postcode)
    source_long = geo.get_long(src_postcode)
    destination_lat = geo.get_lat(dest_postcode)
    destination_long = geo.get_long(dest_postcode)
    distance = geo.distance(source_lat, source_long, destination_lat, destination_long)
    return distance


def get_details_from_last_txn(hbase_connection, card_id, table_name):
    row = hbase_connection.get_data(key=str.encode(card_id), table=table_name)
    return row.get(b'st:pc'), row.get(b'bt:score'), row.get(b'bt:ucl'), row.get(b'st:tdt')


def push_to_hbase(hbase_connection, updated_msg, table, data):
    hbase_connection.write_data(str(updated_msg['card_id']).encode(), data, table)


def check_if_fraud(credit_score):
    if int(credit_score) < 1000:
        return True
    else:
        return False


def check_ucl(ucl, amount):
    if amount < int(ucl):
        return True
    else:
        return False


def check_distance(geo, last_postcode, curr_postcode, last_tdt, curr_tdt):
    distance = get_distance(geo, last_postcode, curr_postcode)
    time = get_time_difference(last_tdt, curr_tdt)
    speed = distance/time

    if speed > speed_threshold:
        return True
    else:
        return False


def execute():
    hbase_connection = dao.HBaseDao()
    geo = GEO_Map()
    conf = ConfigFactory.parse_file('application.conf')

    consumer = kafka_consumer(conf['kafka'])
    for msg in consumer:
        incoming_msg = json.loads(msg.value)
        last_postcode, credit_score, ucl, txn_time = get_details_from_last_txn(hbase_connection,
                                                                               str(incoming_msg['card_id']), "lookup")

        print("========================", last_postcode, credit_score, ucl, txn_time)
        print("========================", type(last_postcode), type(credit_score), type(ucl), type(txn_time))

        incoming_msg['last_postcode'] = last_postcode
        incoming_msg['credit_score'] = int(credit_score)
        incoming_msg['last_txn_time'] = txn_time
        incoming_msg['ucl'] = ucl
        print(incoming_msg)
        if check_if_fraud(credit_score) and check_ucl(ucl, incoming_msg['amount']) and \
                check_distance(geo, last_postcode, incoming_msg['postcode'], txn_time, incoming_msg['transaction_dt']):
            post_code = incoming_msg['postcode']
            txn_time = incoming_msg['transaction_dt']
            data = {
                b'st:pc': str(post_code).encode(),
                b'st:tdt': txn_time.encode()
            }
            push_to_hbase(hbase_connection, incoming_msg, 'lookup_test', data)
            break
        else:
            # Fraud TXN -> update into card_transaction table
            member_id = incoming_msg['member_id']
            amount = incoming_msg['amount']
            post_code = incoming_msg['postcode']
            pos_id = incoming_msg['pos_id']
            txn_time = incoming_msg['transaction_dt']
            status = 'FRAUD'
            data = {
                b'md:m_id': str(member_id).encode(),
                b'td:amt': str(amount).encode(),
                b'st:pc': str(post_code).encode(),
                b'td:pos': str(pos_id).encode(),
                b'st:tdt': txn_time.encode(),
                b'td:st': status.encode()
            }
            push_to_hbase(hbase_connection, incoming_msg,'card_transactions_test', data)
            break

if __name__ == '__main__':
    execute()
    # geo = GEO_Map()
    # curr_postcode = '19352'
    # last_postcode = '44814'
    # curr_txn_time = '28-06-2018 14:57:43'
    # last_txn_time = b'01-02-2018 04:58:38'
    # d = get_distance(geo, curr_postcode, last_postcode)
    # t = get_time_difference(last_txn_time.decode(), curr_txn_time)
    # res = d/t
    # print(res)
    # hbase_connection = dao.HBaseDao()
    # print(get_details_from_last_txn(hbase_connection, '348684315090900', 'lookup'))
