#!/usr/bin/env python3
from cassandra.cluster import Cluster
import argparse
import sys
from ssl import SSLContext, PROTOCOL_TLSv1
from cassandra.auth import PlainTextAuthProvider





def parse_user_args():
    parser = argparse.ArgumentParser()
    parser.description = "Cassandra row counter"
    parser.add_argument("host", type=str, help="Cassandra host")
    parser.add_argument("keyspace", type=str, help="Keyspace to use")
    parser.add_argument("table", type=str, help="Table to use")
    parser.add_argument("key", type=str, help="Key to use, when counting rows")
    parser.add_argument("--split", type=int, default=18, help="Split (see documentation)")
    parser.add_argument("--port", type=int, default=9042, help="Cassandra port (9042 by default)")
    parser.add_argument("--user", type=str, default="cassandra", help="Cassandra username")
    parser.add_argument("--password", type=str, default="cassandra", help="Cassandra password")
    parser.add_argument("--ssl-certificate", dest="ssl_cert", type=str, help="SSL certificate to use")
    parser.add_argument("--ssl-key", type=str, dest="ssl_key", help="Key for the SSL certificate")
    args = parser.parse_args()

    count_rows(args.host, args.keyspace, args.table, args.key, args.split, args.port, args.user, args.password, args.ssl_cert, args.ssl_key)


def count_rows(host, keyspace, table, key, split, port, user, password, ssl_cert, ssl_key):
    ssl_context = SSLContext(PROTOCOL_TLSv1)


    ssl_context.load_cert_chain(
        certfile=ssl_cert,
        keyfile=ssl_key)

    auth_provider  = PlainTextAuthProvider(username=user, password=password)
    cluster = Cluster([host], port=port, ssl_context=ssl_context, auth_provider=auth_provider)
    try:
        session = cluster.connect(keyspace)
    except Exception as e:
        print("Exception when connecting to Cassandra: {}".format(e.args[0]))
        sys.exit(1)

    session.execute("use {}".format(keyspace))

    min_token = -9223372036854775808
    max_token = 9223372036854775807
    primary_key = key

    sql_list = []
    result_list = []

    i = min_token
    while i <= max_token - 1:
        i_max = i + pow(10, split)
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

if __name__ == "__main__":
    parse_user_args()
