import json

from pyhocon import ConfigFactory
from datetime import datetime

import dao
from geo_map import GEO_Map
from kafka_consumer import kafka_consumer


class CredFinance:
    __instance = None

    @staticmethod
    def get_instance():
        if CredFinance.__instance is None:
            CredFinance()
        return CredFinance.__instance

    def __init__(self, file_path=None):
        self.geo = None
        self.hbase_connection = None
        if file_path is None:
            conf_file_path = 'application.conf'
        else:
            conf_file_path = file_path

        self.conf = ConfigFactory.parse_file(conf_file_path)
        self.speed_threshold = float(self.conf['threshold']['speed'])
        self.score_threshold = int(self.conf['threshold']['score'])

    def get_time_difference(self, timestamp1, timestamp2):
        #getting the time difference between last transaction and the current transaction
        format1 = "%d-%m-%Y %H:%M:%S"
        datetime1 = datetime.strptime(timestamp1, format1)
        datetime2 = datetime.strptime(timestamp2, format1)
        difference = datetime2 - datetime1
        time_difference_seconds = difference.total_seconds()
        return time_difference_seconds

    def get_distance(self, src_postcode, des_postcode):
        #getting the distance between last txn and the current txn
        source_lat = self.geo.get_lat(str(src_postcode))
        source_long = self.geo.get_long(str(src_postcode))
        destination_lat = self.geo.get_lat(str(des_postcode))
        destination_long = self.geo.get_long(str(des_postcode))
        distance = self.geo.distance(source_lat, source_long, destination_lat, destination_long)
        return distance

    def get_details_from_last_txn(self, card_id, table_name):
        #pulling the details from hbase
        row = self.hbase_connection.get_data(key=str.encode(card_id), table=table_name)
        return row.get(b'st:pc'), row.get(b'bt:score'), row.get(b'bt:ucl'), row.get(b'st:tdt')

    def push_to_hbase(self, key, table, data):
        #pushing the details to hbase using dao
        self.hbase_connection.write_data(str(key).encode(), data, table)

    def check_if_fraud_score(self, credit_score):
        #checking if the credit score is less than the threshold
        if int(credit_score) < self.score_threshold:
            return True
        else:
            return False

    def check_if_fraud_ucl(self, ucl, amount):
        #checking if the amount is more than the UCL
        if amount > int(ucl):
            return True
        else:
            return False

    def check_if_fraud_speed(self, last_postcode, curr_postcode, last_tdt, curr_tdt):
        #checking if the speed is more than the treshold.
        distance = self.get_distance(last_postcode, curr_postcode)
        time = self.get_time_difference(last_tdt, curr_tdt)
        if time == 0:
            return False
        speed = distance / time
        if speed > self.speed_threshold:
            return True
        else:
            return False

    def check_if_fraud(self, credit_score, ucl, amount, last_postcode, curr_postcode, last_tdt, curr_tdt):

        #checking if all the three condition satisfies
        if self.check_if_fraud_score(credit_score) or self.check_if_fraud_ucl(ucl, amount) or self.check_if_fraud_speed(
                last_postcode,
                curr_postcode,
                last_tdt, curr_tdt):
            return True
        else:
            return False

    def push_to_lookup(self, enriched_message):
        #pushing the updated message to lookup hbase table
        post_code = enriched_message['postcode']
        txn_time = enriched_message['transaction_dt']
        data_for_lookup = {
            b'st:pc': str(post_code).encode(),
            b'st:tdt': txn_time.encode()
        }
        print("Pushing to Lookup for ")
        print(enriched_message)
        self.push_to_hbase(enriched_message['card_id'], self.conf['hbase']['lookup_table'], data_for_lookup)


    def push_to_txn_table(self, enriched_msg, status):
        #pushing the updated message to txn_table
        member_id = enriched_msg['member_id']
        amount = enriched_msg['amount']
        post_code = enriched_msg['postcode']
        pos_id = enriched_msg['pos_id']
        txn_time = enriched_msg['transaction_dt']
        data_for_txn_table = {
            b'md:m_id': str(member_id).encode(),
            b'td:amt': str(amount).encode(),
            b'td:pc': str(post_code).encode(),
            b'td:pos': str(pos_id).encode(),
            b'td:tdt': txn_time.encode(),
            b'td:st': status.encode()
        }
        print("Pushing to TXN_table " + status + " ")
        print(enriched_msg)
        self.push_to_hbase(enriched_msg['card_id'], self.conf['hbase']['card_txn_table'], data_for_txn_table)

    def process_genuine_txn(self, enriched_msg):
        #pushing the txn details if the txn is genuine
        self.push_to_lookup(enriched_msg)
        self.push_to_txn_table(enriched_msg, 'GENUINE')

    def process_fraud_txn(self, enriched_msg):
        #pushing the txn details if the txn is fraud
        self.push_to_txn_table(enriched_msg, 'FRAUD')

    def get_enrich_message(self, incoming_message):
        #pulling the details of card_id from hbase and adding to main message to make the data enriched
        last_postcode, credit_score, ucl, txn_time = self.get_details_from_last_txn(str(incoming_message['card_id']),
                                                                                    self.conf['hbase']['lookup_table'])

        if last_postcode is not None and credit_score is not None and ucl is not None and txn_time is not None:
            incoming_message['last_postcode'] = last_postcode.decode()
            incoming_message['credit_score'] = int(credit_score)
            incoming_message['last_txn_time'] = txn_time.decode()
            incoming_message['ucl'] = ucl
            return incoming_message
        else:
            return None

    def get_connections(self):
        self.hbase_connection = dao.HBaseDao()
        self.geo = GEO_Map()

    def execute(self):

        #getting the conncection from kafka
        consumer = kafka_consumer(self.conf['kafka'])

        for msg in consumer:
            start = datetime.now() #start time
            incoming_msg = json.loads(msg.value)

            #pulling the details of card_id from hbase
            enrich_message = self.get_enrich_message(incoming_msg)


            if enrich_message is None:
                #if there is no record in the hbase, then we mark it as genuine transaction
                self.process_genuine_txn(incoming_msg)
            else:
                if self.check_if_fraud(enrich_message['credit_score'], enrich_message['ucl'], enrich_message['amount'],
                                   enrich_message['last_postcode'], enrich_message['postcode'],
                                   enrich_message['last_txn_time'], enrich_message['transaction_dt']):
                    self.process_fraud_txn(incoming_msg)
                else:
                    self.process_genuine_txn(incoming_msg)

            end = datetime.now()
            print("----------time taken" + str(end - start))

if __name__ == '__main__':
    cred = CredFinance()

    #getting the connection to hbase and geo_map
    cred.get_connections()

    #starting the streaming application
    cred.execute()
