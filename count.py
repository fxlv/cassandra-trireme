#!/usr/bin/env python3
from cassandra.cluster import Cluster
import argparse
import sys
import logging
from ssl import SSLContext, PROTOCOL_TLSv1_2
from cassandra.auth import PlainTextAuthProvider

min_token = -9223372036854775808
max_token = 9223372036854775807


def parse_user_args():
    """Parse commandline arguments."""
    parser = argparse.ArgumentParser()
    parser.description = "Cassandra row counter"
    parser.add_argument("action", type=str,
                        choices=["count-rows", "print-rows", "update-rows", "delete-rows", "find-nulls"],
                        help="What would you like to do?")
    parser.add_argument("host", type=str, help="Cassandra host")
    parser.add_argument("keyspace", type=str, help="Keyspace to use")
    parser.add_argument("table", type=str, help="Table to use")
    parser.add_argument("key", type=str, help="Key to use, when counting rows")
    parser.add_argument("--update-key", type=str, dest="update_key", help="Update key.")
    parser.add_argument("--update-value", type=str, dest="update_value", help="Update value.")
    parser.add_argument("--value-column", type=str, dest="value_column", help="Value column.")
    parser.add_argument("--filter-string", type=str, dest="filter_string", help="Additional filter string. See docs.")
    parser.add_argument("--split", type=int, default=18, help="Split (see documentation)")
    parser.add_argument("--port", type=int, default=9042, help="Cassandra port (9042 by default)")
    parser.add_argument("--user", type=str, default="cassandra", help="Cassandra username")
    parser.add_argument("--password", type=str, default="cassandra", help="Cassandra password")
    parser.add_argument("--ssl-certificate", dest="ssl_cert", type=str, help="SSL certificate to use")
    parser.add_argument("--ssl-key", type=str, dest="ssl_key", help="Key for the SSL certificate")
    parser.add_argument("--debug", action="store_true", help="Enable DEBUG logging")
    args = parser.parse_args()
    return args


def get_cassandra_session(host, port, user, password, ssl_cert, ssl_key):
    """Establish Cassandra connection and return session object."""
    if ssl_cert == None and ssl_key == None:
        # skip setting up ssl
        ssl_context = None
    else:
        ssl_context = SSLContext(PROTOCOL_TLSv1_2)
        ssl_context.load_cert_chain(
            certfile=ssl_cert,
            keyfile=ssl_key)

    auth_provider = PlainTextAuthProvider(username=user, password=password)
    cluster = Cluster([host], port=port, ssl_context=ssl_context, auth_provider=auth_provider)
    try:
        session = cluster.connect()
    except Exception as e:
        print("Exception when connecting to Cassandra: {}".format(e.args[0]))
        sys.exit(1)
    return session


def find_null_cells(session, keyspace, table, key_column, value_column):
    """Scan table looking for 'Null' values in the specified column.

    Finding 'Null' columns in a table.

    'key_column' - the column that cotains some meaningful key/id. Your primary key most likely.
    'value_column' - the column where you wish to search for 'Null'

    Having 'Null' cells in Cassandra is the same as not having them.
    However if you don't control the data model or cannot change it for whatever reason but still want to know
    how many such 'Null' cells you have, you are bit out of luck.
    Filtering by 'Null' is not something that you can do in Cassandra.
    So what you can do is to query them and look for 'Null' in the result.
    """
    session.execute("use {}".format(keyspace))

    sql_template = "select {key},{column} from {keyspace}.{table}"
    result_list = []

    sql = sql_template.format(keyspace=keyspace, table=table, key=key_column, column=value_column)
    logging.debug("Executing: {}".format(sql))
    result = session.execute(sql)
    result_list = [r for r in result if getattr(r, value_column) == None]

    # print("Total amount of rows in {keyspace}.{table} is {sum}".format(keyspace=keyspace, table=table, sum=total_sum))


def batch_sql_query(sql_statement, key_name, key_list):
    """Run a query on the specifies list of primary keys."""

    for key in key_list:
        sql = "{sql_statement} where {key_name} = {key}".format(sql_statement=sql_statement, key_name=key_name, key=key)
        logging.debug("Executing: {}".format(sql))
        result = session.execute(sql)
        logging.debug(result)


def distributed_sql_query(sql_statement, key_column, split, filter_string):
    sql_list = []
    result_list = []

    sql_base_template = "{sql_statement} where token({key_column}) >= {min} and token({key_column}) < {max}"

    if filter_string:
        logging.debug("using additional filter string: {}".format(filter_string))
        sql_base_template += " and {}".format(filter_string)

    # calculate token ranges for distributing the query
    i = min_token
    while i <= max_token - 1:
        i_max = i + pow(10, split)
        if i_max > max_token: i_max = max_token  # don't go higher than max_token
        sql_list.append((i, i_max))
        logging.debug(i_max)
        i = i_max
    logging.info("sql list length is {}".format(len(sql_list)))

    for (min, max) in sql_list:
        sql = sql_base_template.format(sql_statement=sql_statement, min=min, max=max, key_column=key_column)
        logging.debug("Executing: {}".format(sql))
        result = session.execute(sql)
        result_list.append(result)
    return result_list


def reductor(result_set):
    """Do the reduce part of map/reduce and return a list of rows."""
    result_list = []
    for result in result_set:
        for row in result:
            result_list.append(row)
    return result_list


def delete_rows(session, keyspace, table, key, split, filter_string):
    session.execute("use {}".format(keyspace))
    sql_template = "delete from {keyspace}.{table}"
    sql_statement = sql_template.format(keyspace=keyspace, table=table)
    result = distributed_sql_query(sql_statement, key_column=key, split=split, filter_string=filter_string)
    return (reductor(result))


def update_rows(session, keyspace, table, key, update_key, update_value, split, filter_string):
    """Update specified rows by setting 'update_key' to 'update_value'.

    When Updating rows in Cassandra you can't filter by token range.
    So what we do is find all the primary keys for the rows that
    we would like to update, and then run an upadate in a for loop.
    """
    session.execute("use {}".format(keyspace))

    rows = get_rows(session, keyspace, table, key, split, update_key, filter_string)
    update_list = []
    for row in rows:
        update_list.append(getattr(row, key))
    logging.info("Updating {} rows".format(len(update_list)))
    logging.info(
        "Updating rows and setting {update_key} to new value {update_value} where filtering string is: {filter_string}".format(
            update_key=update_key, update_value=update_value, filter_string=filter_string))

    sql_template = "update {keyspace}.{table} set {update_key} = {update_value}"
    sql_statement = sql_template.format(keyspace=keyspace, table=table, update_key=update_key,
                                        update_value=update_value)
    result = batch_sql_query(sql_statement, key, update_list)
    logging.info("Operation complete.")


def get_rows(session, keyspace, table, key, split, value_column=None, filter_string=None):
    session.execute("use {}".format(keyspace))

    if not value_column:
        select_values = "*"
    else:
        select_values = "{key}, {value_column}".format(key=key, value_column=value_column)

    sql_template = "select {select_values} from {keyspace}.{table}"

    sql_statement = sql_template.format(select_values=select_values, keyspace=keyspace, table=table)
    result = distributed_sql_query(sql_statement, key_column=key, split=split, filter_string=filter_string)
    return (reductor(result))


def get_rows_count(session, keyspace, table, key, split, filter_string=None):
    session.execute("use {}".format(keyspace))

    sql_template = "select count(*) from {keyspace}.{table}"

    sql_statement = sql_template.format(keyspace=keyspace, table=table)
    result = distributed_sql_query(sql_statement, key_column=key, split=split, filter_string=filter_string)
    count = 0
    for r in result:
        count += r[0].count
    return count


def print_rows(session, keyspace, table, key, split, value_column=None, filter_string=None):
    rows = get_rows(session, keyspace, table, key, split, value_column, filter_string)
    for row in rows:
        print(row)


def print_rows_count(session, keyspace, table, key, split, filter_string=None):
    count = get_rows_count(session, keyspace, table, key, split, filter_string)
    print("Total amount of rows in {keyspace}.{table} is {count}".format(
        keyspace=keyspace, table=table, count=count))


if __name__ == "__main__":

    args = parse_user_args()

    if args.debug:
        logging.basicConfig(level=logging.DEBUG)
        logging.debug('Logging started.')
    else:
        logging.basicConfig(level=logging.INFO)

    session = get_cassandra_session(args.host, args.port, args.user, args.password, args.ssl_cert, args.ssl_key)

    if args.action == "find-nulls":
        find_null_cells(session, args.keyspace, args.table, "id", "comment")
    elif args.action == "count-rows":
        print_rows_count(session, args.keyspace, args.table, args.key, args.split, args.filter_string)
    elif args.action == "print-rows":
        print_rows(session, args.keyspace, args.table, args.key, args.split, args.value_column, args.filter_string)
    elif args.action == "update-rows":
        update_rows(session, args.keyspace, args.table, args.key, args.update_key, args.update_value, args.split,
                    args.filter_string)

    elif args.action == "delete-rows":
        delete_rows(session, args.keyspace, args.table, args.key, args.split, args.filter_string)

    else:
        # this won't be accepted by argparse anyways
        sys.exit(1)
