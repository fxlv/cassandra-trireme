#!/usr/bin/env python3
#
# Trireme
#
# Cassandra database row counter and manipulator.
#
# kaspars@fx.lv
#
import argparse
import datetime
import logging
import queue
import sys
import threading
import time
from ssl import SSLContext, PROTOCOL_TLSv1, PROTOCOL_TLSv1_2

from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster

default_min_token = -9223372036854775808
default_max_token = 9223372036854775807


class Result:
    def __init__(self, min, max, value):
        self.min = min
        self.max = max
        self.value = value

    def __str__(self):
        return "Result(min: {}, max: {}, value: {})".format(
            self.min, self.max, self.value)

class Settings:
    def __init__(self):
        pass

class Token_range:
    def __init__(self, min, max):
        self.min = min
        self.max = max


def parse_user_args():
    """Parse commandline arguments."""
    parser = argparse.ArgumentParser()
    parser.description = "Trireme - Cassandra row manipulator"
    parser.add_argument("action",
                        type=str,
                        choices=[
                            "count-rows", "print-rows", "update-rows",
                            "delete-rows", "find-nulls", "find-wide-partitions"
                        ],
                        help="What would you like to do?")
    parser.add_argument("host", type=str, help="Cassandra host")
    parser.add_argument("keyspace", type=str, help="Keyspace to use")
    parser.add_argument("table", type=str, help="Table to use")
    parser.add_argument("key", type=str, help="Key to use, when counting rows")
    parser.add_argument("--extra-key",
                        type=str,
                        dest="extra_key",
                        help="Extra key, in case of compound primary key.")
    parser.add_argument("--update-key",
                        type=str,
                        dest="update_key",
                        help="Update key.")
    parser.add_argument("--update-value",
                        type=str,
                        dest="update_value",
                        help="Update value.")
    parser.add_argument("--value-column",
                        type=str,
                        dest="value_column",
                        help="Value column.")
    parser.add_argument("--filter-string",
                        type=str,
                        dest="filter_string",
                        help="Additional filter string. See docs.")
    parser.add_argument("--split",
                        type=int,
                        default=18,
                        help="Split (see documentation)")
    parser.add_argument("--port",
                        type=int,
                        default=9042,
                        help="Cassandra port (9042 by default)")
    parser.add_argument("--user",
                        type=str,
                        default="cassandra",
                        help="Cassandra username")
    parser.add_argument("--password",
                        type=str,
                        default="cassandra",
                        help="Cassandra password")
    parser.add_argument("--ssl-certificate",
                        dest="ssl_cert",
                        type=str,
                        help="SSL certificate to use")
    parser.add_argument("--ssl-key",
                        type=str,
                        dest="ssl_key",
                        help="Key for the SSL certificate")
    parser.add_argument("--ssl-use-tls-v1",
                        action="store_true",
                        dest="ssl_v1",
                        help="Use TLS1 instead of 1.2")

    parser.add_argument("--debug",
                        action="store_true",
                        help="Enable DEBUG logging")

    parser.add_argument("--min-token", type=int,
                       help="Min token")

    parser.add_argument("--max-token", type=int,
                       help="Max token")
    args = parser.parse_args()
    return args


def get_cassandra_session(host,
                          port,
                          user,
                          password,
                          ssl_cert,
                          ssl_key,
                          ssl_v1=False):
    """Establish Cassandra connection and return session object."""
    if ssl_cert is None and ssl_key is None:
        # skip setting up ssl
        ssl_context = None
    else:
        if ssl_v1:
            tls_version = PROTOCOL_TLSv1
        else:
            tls_version = PROTOCOL_TLSv1_2
        ssl_context = SSLContext(tls_version)
        ssl_context.load_cert_chain(certfile=ssl_cert, keyfile=ssl_key)

    auth_provider = PlainTextAuthProvider(username=user, password=password)
    cluster = Cluster([host],
                      port=port,
                      ssl_context=ssl_context,
                      auth_provider=auth_provider)
    try:
        session = cluster.connect()
    except Exception as e:
        print("Exception when connecting to Cassandra: {}".format(e.args[0]))
        sys.exit(1)
    return session


def find_null_cells(session, keyspace, table, key_column, value_column):
    """Scan table looking for 'Null' values in the specified column.

    Finding 'Null' columns in a table.

    'key_column' - the column that cotains some meaningful key/id.
    Your primary key most likely.
    'value_column' - the column where you wish to search for 'Null'

    Having 'Null' cells in Cassandra is the same as not having them.
    However if you don't control the data model or cannot change it
    for whatever reason but still want to know
    how many such 'Null' cells you have, you are bit out of luck.
    Filtering by 'Null' is not something that you can do in Cassandra.
    So what you can do is to query them and look for 'Null' in the result.
    """

    # TODO: this is just a stub for now, not fully implemented

    session.execute("use {}".format(keyspace))

    sql_template = "select {key},{column} from {keyspace}.{table}"
    result_list = []

    sql = sql_template.format(keyspace=keyspace,
                              table=table,
                              key=key_column,
                              column=value_column)
    logging.debug("Executing: {}".format(sql))
    result = session.execute(sql)
    result_list = [r for r in result if getattr(r, value_column) is None]


def batch_sql_query(sql_statement, key_name, key_list, dry_run=False):
    """Run a query on the specifies list of primary keys."""

    for key in key_list:
        if isinstance(key, dict):
            sql = "{sql_statement} where ".format(sql_statement=sql_statement)
            andcount = 0
            for k in key:
                value = key[k]
                if isinstance(value, str):
                    value = "'{}'".format(value)
                sql += "{key_name} = {key}".format(key_name=k, key=value)
                if andcount < 1:
                    andcount += 1
                    sql += " and "
        else:
            sql = "{sql_statement} where {key_name} = {key}".format(
                sql_statement=sql_statement, key_name=key_name, key=key)

        logging.debug("Executing: {}".format(sql))
        if dry_run:
            logging.info("Would execute: {}".format(sql))
        else:
            result = session.execute(sql)
            logging.debug(result)
            time.sleep(0.1)

def batch_delete(sql_statement, delete_list, dry_run=False):
    """Run a query on the specifies list of primary keys."""

    for dictionary in delete_list:
        sql = "{sql_statement} where ".format(sql_statement=sql_statement)

        andcount = 0
        for key in dictionary:
            value = dictionary[key]
            # cassandra is timezone aware, however the response that we would have received
            # previously does not contain timezone, so we need to add it manually
            if isinstance(value, datetime.datetime):
                value = value.replace(tzinfo=datetime.timezone.utc)
            value = "'{}'".format(value)
            sql += "{key_name} = {key}".format(key_name=key, key=value)
            if andcount < 1:
                andcount += 1
                sql += " and "

        logging.debug("Executing: {}".format(sql))
        if dry_run:
            logging.info("Would execute: {}".format(sql))
        else:
            result = session.execute(sql)
            logging.debug(result)
            time.sleep(0.1)

def seconds_to_human(seconds):
    # default values
    hours = 0
    minutes = 0

    # find minutes and the reminder of seconds
    if seconds >= 60:
        remaining_seconds = seconds % 60
        minutes = round((seconds - remaining_seconds) / 60)
        seconds = remaining_seconds
    if minutes >= 60:
        remaining_minutes = minutes % 60
        hours = round((minutes - remaining_minutes) / 60)
        minutes = remaining_minutes
    return hours, minutes, seconds


def human_time(seconds):
    hours, minutes, seconds = seconds_to_human(seconds)

    if hours:
        human_time_string = "{} hours, {} minutes, {} seconds".format(
            hours, minutes, seconds)
    elif minutes:
        human_time_string = "{} hours, {} minutes, {} seconds".format(
            hours, minutes, seconds)
    else:
        human_time_string = "{} hours, {} minutes, {} seconds".format(
            hours, minutes, seconds)

    return human_time_string


def sql_query(sql_statement, key_column, result_list, failcount, sql_list,
              filter_string, kill_queue, extra_key):
    while len(sql_list) > 0:
        if kill_queue.qsize() > 0:
            logging.warning("Aborting query on request.")
            return
        (min, max) = sql_list.pop()
        if extra_key:
            sql_base_template = "{sql_statement} where token({key_column}, {extra_key}) " \
                                ">= {min} and token({key_column}, {extra_key}) < {max}"
        else:
            sql_base_template = "{sql_statement} where token({key_column}) " \
                            ">= {min} and token({key_column}) < {max}"
        if filter_string:
            sql_base_template += " and {}".format(filter_string)
        sql = sql_base_template.format(sql_statement=sql_statement,
                                       min=min,
                                       max=max,
                                       key_column=key_column, extra_key=extra_key)
        try:
            if result_list.qsize() % 100 == 0:
                logging.debug("Executing: {}".format(sql))
            result = session.execute(sql)
            r = Result(min, max, result)
            result_list.put(r)
        except Exception as e:
            failcount += 1
            logging.warning(
                            "Got Cassandra exception: "
                            "{msg} when running query: {sql}"
                            .format(sql=sql, msg=e))


def distributed_sql_query(sql_statement,
                          key_column,
                          split,
                          filter_string,
                          token_range=None, extra_key=None):
    start_time = datetime.datetime.now()
    sql_list = []
    result_list = queue.Queue()
    failcount = 0
    thread_count = 15
    tr = token_range
    # calculate token ranges for distributing the query
    i = tr.min
    logging.debug("Preparing splits...")
    while i <= tr.max - 1:
        i_max = i + pow(10, split)
        if i_max > tr.max:
            i_max = tr.max  # don't go higher than max_token
        sql_list.append((i, i_max))
        i = i_max
    logging.debug("sql list length is {}".format(len(sql_list)))
    time.sleep(1)
    process_queue = queue.Queue()
    kill_queue = queue.Queue()  # TODO: change this to an event?

    tm = None
    try:
        while len(sql_list) > 0:
            if process_queue.qsize() < thread_count:
                thread = threading.Thread(
                    target=sql_query,
                    args=(sql_statement, key_column, result_list, failcount,
                          sql_list, filter_string, kill_queue, extra_key))
                thread.start()
                logging.debug("Started thread {}".format(thread))
                process_queue.put(thread)
            else:
                logging.debug("Max process count reached")
                logging.debug("{} more queries remaining".format(
                    len(sql_list)))
                res_count = result_list.qsize()
                logging.debug("{} results so far".format(res_count))
                n = datetime.datetime.now()
                delta = n - start_time
                elapsed_time = delta.total_seconds()
                logging.debug("Elapsed time: {}.".format(
                    human_time(elapsed_time)))
                if res_count > 0:
                    result_per_sec = res_count / elapsed_time
                    logging.debug("{} results / s".format(result_per_sec))
                time.sleep(10)
        logging.debug("No more work left, waiting for all threads to stop.")
        # TODO: maybe instead send the kill pill
        #  or check thread liveliness in some other way
        # instead of just blindly waiting for a sec
        time.sleep(1)
    except KeyboardInterrupt:
        logging.warning("Ctrl+c pressed, asking all threads to stop.")
        kill_queue.put(0)
        time.sleep(2)
        logging.debug("{} more queries remaining".format(len(sql_list)))
        logging.debug("{} results so far".format(res_count))

    if failcount > 0:
        logging.warning(
            "There were {} failures during the query.".format(failcount))
    return result_list


def reductor(result_set):
    """Do the reduce part of map/reduce and return a list of rows."""
    result_list = []
    while result_set.qsize() > 0:
        result = result_set.get()
        for row in result.value:
            result_list.append(row)
    return result_list


def delete_rows(session, keyspace, table, key, split, filter_string, tr, extra_key=None):
    session.execute("use {}".format(keyspace))
    rows = get_rows(session, keyspace, table, key, split, tr, value_column=None, filter_string=filter_string, extra_key=extra_key)
    delete_list = []
    logging.debug("Total row count: {}".format(len(rows)))
    logging.debug("Token range used: min={}, max={}".format(tr.min, tr.max))
    for row in rows:
        logging.debug("ROW: {}".format(row))

        if extra_key:
            delete_list.append({
                key: getattr(row, key),
                extra_key: getattr(row, extra_key)
            })  # use tuple of key, extra_key
        else:
            delete_list.append(getattr(row, key))
    logging.info("Deleting {} rows".format(len(delete_list)))

    sql_template = "delete from {keyspace}.{table}"
    sql_statement = sql_template.format(keyspace=keyspace,
                                        table=table)
    logging.info(sql_statement)

    while True:
        response = input(
            "Are you sure you want to continue? (y/n)").lower().strip()
        if response == "y":
            break
        elif response == "n":
            logging.warning("Aborting upon user request")
            return 1
    result = batch_delete(sql_statement, delete_list, False)
    logging.info("Operation complete.")



def update_rows(session,
                keyspace,
                table,
                key,
                update_key,
                update_value,
                split,
                filter_string,
                extra_key=None):
    """Update specified rows by setting 'update_key' to 'update_value'.

    When Updating rows in Cassandra you can't filter by token range.
    So what we do is find all the primary keys for the rows that
    we would like to update, and then run an update in a for loop.
    """
    session.execute("use {}".format(keyspace))
    rows = get_rows(session, keyspace, table, key, split, update_key,
                    filter_string, extra_key)
    update_list = []
    for row in rows:
        if extra_key:
            update_list.append({
                key: getattr(row, key),
                extra_key: getattr(row, extra_key)
            })  # use tuple of key, extra_key
        else:
            update_list.append(getattr(row, key))
    logging.info("Updating {} rows".format(len(update_list)))
    logging.info(
                "Updating rows and setting {update_key} to new value "
                "{update_value} where filtering string is: {filter_string}"
                .format(update_key=update_key,
                        update_value=update_value,
                        filter_string=filter_string))

    # surround update value with quotes in case it is a string,
    # but don't do it if it looks like a string
    # but in reality is meant to be a a boolean
    booleans = ["true", "false"]
    if isinstance(update_value, str):
        if update_value.lower() not in booleans:
            update_value = "'{}'".format(update_value)

    sql_template = "update {keyspace}.{table} set "\
                   "{update_key} = {update_value}"
    sql_statement = sql_template.format(keyspace=keyspace,
                                        table=table,
                                        update_key=update_key,
                                        update_value=update_value)
    logging.info(sql_statement)

    while True:
        response = input(
            "Are you sure you want to continue? (y/n)").lower().strip()
        if response == "y":
            break
        elif response == "n":
            logging.warning("Aborting upon user request")
            return 1
    result = batch_sql_query(sql_statement, key, update_list, False)
    logging.info("Operation complete.")


def get_rows(session,
             keyspace,
             table,
             key,
             split,
             tr,
             value_column=None,
             filter_string=None,
             extra_key=None):
    session.execute("use {}".format(keyspace))

    if not value_column:
        select_values = "*"
    else:
        if extra_key:
            select_values = "{key}, {extra_key}, {value_column}".format(
                key=key, extra_key=extra_key, value_column=value_column)
        else:
            select_values = "{key}, {value_column}".format(
                key=key, value_column=value_column)

    sql_template = "select {select_values} from {keyspace}.{table}"

    sql_statement = sql_template.format(select_values=select_values,
                                        keyspace=keyspace,
                                        table=table)
    result = distributed_sql_query(sql_statement,
                                   key_column=key,
                                   split=split,
                                   filter_string=filter_string, token_range=tr, extra_key=extra_key)
    return (reductor(result))


def get_rows_count(session,
                   keyspace,
                   table,
                   key,
                   split,
                   filter_string=None,
                   aggregate=True,
                   token_range=None):
    session.execute("use {}".format(keyspace))

    sql_template = "select count(*) from {keyspace}.{table}"

    sql_statement = sql_template.format(keyspace=keyspace, table=table)
    result = distributed_sql_query(sql_statement,
                                   key_column=key,
                                   split=split,
                                   filter_string=filter_string,
                                   token_range=token_range)
    count = 0

    unaggregated_count = []
    while result.qsize() > 0:
        r = result.get()
        if aggregate:
            count += r.value[0].count
        else:
            split_count = Result(r.min, r.max, r.value[0])
            unaggregated_count.append(split_count)
    if aggregate:
        return count
    else:
        return unaggregated_count


def print_rows(session,
               keyspace,
               table,
               key,
               split,
               value_column=None,
               filter_string=None, extra_key=None):
    rows = get_rows(session, keyspace, table, key, split, value_column,
                    filter_string, extra_key)
    for row in rows:
        print(row)


def find_wide_partitions(session,
                         keyspace,
                         table,
                         key,
                         split,
                         value_column=None,
                         filter_string=None):
    # select count(*) from everywhere, record all the split sizes
    # get back a list of dictionaries [ {'min': 123, 'max',124, 'count':1 } ]
    # sort it by 'count' and show top 5 or something

    # get rows count, but don't aggregate
    count = get_rows_count(session, keyspace, table, key, split, filter_string,
                           False)
    # now we have count of rows per split, let's sort it
    count.sort(key=lambda x: x.value, reverse=True)
    # now that we know the most highly loaded splits, we can drill down
    most_loaded_split = count[0]
    token_range = Token_range(most_loaded_split.min, most_loaded_split.max)
    most_loaded_split_count = get_rows_count(session,
                                             keyspace,
                                             table,
                                             key,
                                             split=14,
                                             filter_string=None,
                                             aggregate=False,
                                             token_range=token_range)
    most_loaded_split_count.sort(key=lambda x: x.value, reverse=True)

    token_range = Token_range(most_loaded_split_count[0].min,
                              most_loaded_split_count[0].max)

    most_loaded_split_count2 = get_rows_count(session,
                                              keyspace,
                                              table,
                                              key,
                                              split=12,
                                              filter_string=None,
                                              aggregate=False,
                                              token_range=token_range)
    most_loaded_split_count2.sort(key=lambda x: x.value, reverse=True)

    token_range = Token_range(most_loaded_split_count2[0].min,
                              most_loaded_split_count2[0].max)

    most_loaded_split_count3 = get_rows_count(session,
                                              keyspace,
                                              table,
                                              key,
                                              split=10,
                                              filter_string=None,
                                              aggregate=False,
                                              token_range=token_range)
    most_loaded_split_count3.sort(key=lambda x: x.value, reverse=True)

    # narrow it down to 100 million split size
    token_range = Token_range(most_loaded_split_count3[0].min,
                              most_loaded_split_count3[0].max)

    most_loaded_split_count4 = get_rows_count(session,
                                              keyspace,
                                              table,
                                              key,
                                              split=8,
                                              filter_string=None,
                                              aggregate=False,
                                              token_range=token_range)
    most_loaded_split_count4.sort(key=lambda x: x.value, reverse=True)

    # narrow it down to 1 million split size
    token_range = Token_range(most_loaded_split_count4[0].min,
                              most_loaded_split_count4[0].max)

    most_loaded_split_count5 = get_rows_count(session,
                                              keyspace,
                                              table,
                                              key,
                                              split=6,
                                              filter_string=None,
                                              aggregate=False,
                                              token_range=token_range)
    most_loaded_split_count5.sort(key=lambda x: x.value, reverse=True)

    # narrow it down to 1 thousand split size
    token_range = Token_range(most_loaded_split_count5[0].min,
                              most_loaded_split_count5[0].max)

    most_loaded_split_count6 = get_rows_count(session,
                                              keyspace,
                                              table,
                                              key,
                                              split=3,
                                              filter_string=None,
                                              aggregate=False,
                                              token_range=token_range)
    most_loaded_split_count6.sort(key=lambda x: x.value, reverse=True)

    # narrow it down to 10 split size
    token_range = Token_range(most_loaded_split_count6[0].min,
                              most_loaded_split_count6[0].max)

    most_loaded_split_count7 = get_rows_count(session,
                                              keyspace,
                                              table,
                                              key,
                                              split=1,
                                              filter_string=None,
                                              aggregate=False,
                                              token_range=token_range)
    most_loaded_split_count7.sort(key=lambda x: x.value, reverse=True)

    print(most_loaded_split)
    print(most_loaded_split_count[0])
    print(most_loaded_split_count2[0])
    print(most_loaded_split_count3[0])
    print(most_loaded_split_count4[0])  # 100 million precision
    print(most_loaded_split_count5[0])  # 1 million precision
    print(most_loaded_split_count6[0])  # 1 thousand precision
    print(most_loaded_split_count7[0])  # 10 precision

    # .......


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

    session = get_cassandra_session(args.host, args.port, args.user,
                                    args.password, args.ssl_cert, args.ssl_key,
                                    args.ssl_v1)

    if args.min_token and args.max_token:
        tr = Token_range(args.min_token, args.max_token)
    else:
        tr = Token_range(default_min_token, default_max_token)

    if args.action == "find-nulls":
        find_null_cells(session, args.keyspace, args.table, "id", "comment")
    elif args.action == "count-rows":
        print_rows_count(session, args.keyspace, args.table, args.key,
                         args.split, args.filter_string)
    elif args.action == "print-rows":
        print_rows(session, args.keyspace, args.table, args.key, args.split,
                   args.value_column, args.filter_string, args.extra_key)
    elif args.action == "find-wide-partitions":
        find_wide_partitions(session, args.keyspace, args.table, args.key,
                             args.split, args.value_column, args.filter_string)
    elif args.action == "update-rows":
        update_rows(session, args.keyspace, args.table, args.key,
                    args.update_key, args.update_value, args.split,
                    args.filter_string, args.extra_key)
    elif args.action == "delete-rows":
        delete_rows(session, args.keyspace, args.table, args.key, args.split,
                    args.filter_string, tr, args.extra_key)

    else:
        # this won't be accepted by argparse anyways
        sys.exit(1)
