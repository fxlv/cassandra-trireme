#!/usr/bin/env python3
from cassandra.cluster import Cluster


cluster = Cluster(['127.0.0.1'], port=9042)
session = cluster.connect('test1')
session.execute('use test1')

min_token = -9223372036854775808
max_token = 9223372036854775807
keyspace = "test1"
table = "testtable"
primary_key = "id"

sql_list = []
result_list = []

i = min_token
while i <= max_token - 1:
    i_max = i + pow(10, 15)  # quadrillion
    #i_max = i + pow(10,18) # quintillion
    if i_max > max_token: i_max = max_token  # don't go higher than max_token
    sql_list.append((i, i_max))
    print(i_max)
    i = i_max
print("sql list length is {}".format(len(sql_list)))
###

sql_template = "select count(*) from {keyspace}.{table} where token({primary_key}) >= {min} and token({primary_key}) < {max}"

total_sum = 0

for (min, max) in sql_list:
    sql = sql_template.format(keyspace=keyspace,
                              table=table,
                              min=min,
                              max=max,
                              primary_key=primary_key)
    print("Executing: {}".format(sql))
    result = session.execute(sql)[0]
    result_list.append(result.count)
    total_sum += result.count

print("Total amount of rows in {keyspace}.{table} is {sum}".format(
    keyspace=keyspace, table=table, sum=total_sum))
