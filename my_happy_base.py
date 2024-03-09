# def look_up_hbase(hbase_connection, hbase_conf, list_of_keys):
#     connection = hbase_connection.value
#     table = connection.table(hbase_conf['table_name'])
#
#     rows = table.row(list_of_keys)
#
#     return rows
#
#
# def write_to_hbase(hbase_connection, hbase_conf, data_to_insert):
#     connection = hbase_connection.value
#
#     table = connection.table(hbase_conf['table_name'])
#
#     # Define data to be inserted
#     # data_to_insert = {
#     #     b'row_key1': {b'column_family1:column1': b'value1', b'column_family2:column2': b'value2'},
#     #     b'row_key2': {b'column_family1:column1': b'value3', b'column_family2:column2': b'value4'},
#     # }
#
#     with table.batch() as batch:
#         for row_key, data in data_to_insert.items():
#             batch.put(row_key, data)
