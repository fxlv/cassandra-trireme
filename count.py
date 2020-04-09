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
import multiprocessing
import time
import platform
import os
import random
from ssl import SSLContext, PROTOCOL_TLSv1, PROTOCOL_TLSv1_2

from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster
from cassandra.policies import DCAwareRoundRobinPolicy
from datastructures import Result, RowForDeletion, Token_range, Mapper_task, Queues, RuntimeSettings, CassandraSettings, \
    CassandraWorkerTask
from presentation import human_time

# settings
from settings import default_min_token, default_max_token


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
    parser.add_argument("--workers",
                        type=int,
                        default=1,
                        help="Amount of worker processes to use")
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
    parser.add_argument("--datacenter", type=str, default=None, help="Prefer this datacenter and use DCAwareRoundRobinPolicy")
    parser.add_argument("--ssl-ca-cert", dest="cacert", type=str, default=None, help="CA cert to use")
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
                          dc, cacert,
                          ssl_v1=False):
    """Establish Cassandra connection and return session object."""

    auth_provider = PlainTextAuthProvider(username=user, password=password)

    py_version = platform.python_version_tuple()


    if ssl_cert is None and ssl_key is None:
        # skip setting up ssl
        ssl_context = None
        cluster = Cluster([host],
                          port=port,
                          auth_provider=auth_provider)
    else:
        if ssl_v1:
            tls_version = PROTOCOL_TLSv1
        else:
            tls_version = PROTOCOL_TLSv1_2

        if int(py_version[0]) == 3 and int(py_version[1]) > 4:
            ssl_context = SSLContext(tls_version)
            ssl_context.load_cert_chain(certfile=ssl_cert, keyfile=ssl_key)
            if cacert:
                ssl_context.load_verify_locations(cacert)
            if dc:
                cluster = Cluster([host],
                                  port=port, load_balancing_policy=DCAwareRoundRobinPolicy(local_dc=dc),
                                  ssl_context=ssl_context,
                                  auth_provider=auth_provider)
            else:
                cluster = Cluster([host],
                                  port=port,
                                  ssl_context=ssl_context,
                                  auth_provider=auth_provider)
        else:
            ssl_options = {'certfile': ssl_cert,
                           'keyfile': ssl_key,
                           'ssl_version': PROTOCOL_TLSv1_2}
            cluster = Cluster([host],
                              port=port,
                              ssl_options=ssl_options,
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


def execute_statement(sql_statement):
    logging.debug("Deleting: {}".format(sql_statement))
    result = session.execute(sql_statement)
    return result


def process_reaper(process_queue):
    max_attempts = 10
    current = 0
    logging.debug("Process reaper: there are {} processes in the queue".format(process_queue.qsize()))
    while process_queue.qsize() > 0:
        if current == max_attempts:
            logging.debug("Process reaper exiting.")
            break
        current +=1
        process = process_queue.get()
        if process.is_alive():
            logging.debug("Process {} is still running, putting back into queue".format(process))
            process_queue.put(process)
        else:
            logging.debug("Reaping process {}".format(process))

def batch_executer(cas_settings,batch_q, batch_result_q):
    logging.info("STARTING batch executor with batch q size: {}".format(batch_q.qsize()))
    s = get_cassandra_session(cas_settings[0],cas_settings[1],cas_settings[2],cas_settings[3],cas_settings[4],cas_settings[5],cas_settings[6])
    time.sleep(10)
    while batch_q.qsize() >0:
        try:
            (min, max, sql) = batch_q.get()
            logging.info("Executing via BATCH: {}".format(sql))
            result = s.execute(sql)
            r = Result(min, max, result)
            batch_result_q.put(r)
            logging.info("Result: {}".format(r))
        except Exception as e:
            logging.warning(
                "Got Cassandra exception: "
                "{msg} when running query: {sql}"
                    .format(sql=sql, msg=e))

def sql_query_q(cas_settings,delete_queue,getter_counter,sql_statement, key_column, result_list, failcount, split_queue,
              filter_string, kill_queue, extra_key):
    while True:
        if kill_queue.qsize() > 0:
            logging.warning("Aborting query on request.")
            return
        if split_queue.qsize() >0:
            if delete_queue.qsize() > 2000: # TODO: 2000 should be enough for anyone, right? :)
                # slow down with SELECTS if the DELETE queue is already big,
                # as there is no point running if DELETE is not keeping up
                time.sleep(1)

            if extra_key:
                sql_base_template = "{sql_statement} where token({key_column}, {extra_key}) " \
                                    ">= {min} and token({key_column}, {extra_key}) < {max}"
            else:
                sql_base_template = "{sql_statement} where token({key_column}) " \
                                    ">= {min} and token({key_column}) < {max}"
            if filter_string:
                sql_base_template += " and {}".format(filter_string)


            # prepare query for execution and then based on queue size, either execute within this thread or delegate in a batch to a separate process

            if split_queue.qsize() > 1000:
                # do the batch approach and get a list of splits from the queue
                batch_q = multiprocessing.Queue()
                batch_result_q = multiprocessing.Queue()
                for i in range(100):
                    (min, max) = split_queue.get()
                    sql = sql_base_template.format(sql_statement=sql_statement,
                                                   min=min,
                                                   max=max,
                                                   key_column=key_column, extra_key=extra_key)
                    batch_q.put((min, max, sql))
                    p = multiprocessing.Process(target=batch_executer, args=(cas_settings,batch_q, batch_result_q))
                    p.start()

                    logging.info("Batch finished: {} / {} ".format(batch_q.qsize(), batch_result_q.qsize()))

            else:
                # handle query here in the thread
                (min, max) = split_queue.get()

                sql = sql_base_template.format(sql_statement=sql_statement,
                                               min=min,
                                               max=max,
                                               key_column=key_column, extra_key=extra_key)
                try:
                    if result_list.qsize() % 100 == 0:
                        logging.debug("Executing: {}".format(sql))
                    result = session.execute(sql)
                    getter_counter.put(0)
                    r = Result(min, max, result)
                    result_list.put(r)
                except Exception as e:
                    failcount += 1
                    logging.warning(
                                    "Got Cassandra exception: "
                                    "{msg} when running query: {sql}"
                                    .format(sql=sql, msg=e))
        else:
            logging.debug("Stopping getter thread due to zero split queue size.")
            break

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

def split_predicter(tr, split):
    # how many splits will there be?
    predicted_split_count = (tr.max - tr.min) / pow(10, split)
    return predicted_split_count

def splitter(queues, rsettings):
    tr = rsettings.tr
    i = tr.min
    predicted_split_count = split_predicter(tr,rsettings.split)
    logging.info("Preparing splits with split size {}".format(rsettings.split))
    logging.info("Predicted split count is {} splits".format(predicted_split_count))
    splitcounter = 0
    while i <= tr.max - 1:
        if queues.split_queue.full():
            logging.debug("There are {} splits prepared. Pausing for a second.".format(splitcounter))
            time.sleep(0.5)
        else:
            i_max = i + pow(10, rsettings.split)
            if i_max > tr.max:
                i_max = tr.max  # don't go higher than max_token
            queues.split_queue.put((i, i_max))
            queues.stats_queue_splits.put(0)
            splitcounter+=1
            i = i_max

    # kill pill for split queue, signaling that we are done
    queues.split_queue.put(False)
    logging.debug("Splitter is done. All splits created")


def distributed_sql_query(sql_statement, cas_settings, queues, rsettings):
    start_time = datetime.datetime.now()
    result_list = result_queue
    failcount = 0
    thread_count = 1
    kill_queue = queue.Queue()  # TODO: change this to an event?

    backoff_counter = 0
    tm = None
    try:
        while True:
            if split_queue.qsize() >0:
                if backoff_counter >0:
                    backoff_counter =0 # reset backoff counter

                if get_process_queue.qsize() < thread_count:
                    thread = threading.Thread(
                        target=sql_query_q,
                        args=(cas_settings,delete_queue,getter_counter,sql_statement, key_column, result_list, failcount,
                              split_queue, filter_string, kill_queue, extra_key))
                    thread.start()
                    logging.info("Started thread {}".format(thread))
                    get_process_queue.put(thread)
                else:
                    logging.info("Max process count reached")
                    logging.info("{} more queries remaining".format(split_queue.qsize()))
                    res_count = result_list.qsize()
                    logging.info("{} results so far".format(res_count))
                    n = datetime.datetime.now()
                    delta = n - start_time
                    elapsed_time = delta.total_seconds()
                    logging.info("Elapsed time: {}.".format(
                        human_time(elapsed_time)))
                    if res_count > 0:
                        result_per_sec = res_count / elapsed_time
                        logging.info("{} results / s".format(result_per_sec))
                    time.sleep(10)
            else:
                backoff_counter += 1
                logging.debug("No splits in the split queue. Will sleep {} sec".format(backoff_counter))
                time.sleep(backoff_counter)
                process_reaper(get_process_queue)

    except KeyboardInterrupt:
        logging.warning("Ctrl+c pressed, asking all threads to stop.")
        kill_queue.put(0)
        time.sleep(2)
        logging.info("{} more queries remaining".format(split_queue.qsize()))
        logging.info("{} results so far".format(res_count))

    if failcount > 0:
        logging.warning(
            "There were {} failures during the query.".format(failcount))
    return result_list

def threaded_reductor(input_queue, output_queue):
    """Do the reduce part of map/reduce and return a list of rows."""
    backoff_timer = 0
    while True:
        if input_queue.qsize() == 0:
            backoff_timer+=1
            logging.debug("No results to reduce, reducer waiting for {} sec".format(backoff_timer))
            time.sleep(backoff_timer)
        else:
            if backoff_timer >0:
                backoff_timer = 0
            result = input_queue.get()
            for row in result.value:
                # for deletion, we want to be token range aware, so we pass token range information as well
                rd = RowForDeletion(result.min, result.max, row)
                output_queue.put(rd)


def delete_preparer(delete_preparer_queue, delete_queue, keyspace, table, key, extra_key):
    sql_template = "delete from {keyspace}.{table}"
    sql_statement = sql_template.format(keyspace=keyspace, table=table)
    backoff_timer=0
    while True:
        if delete_preparer_queue.qsize() == 0:
            backoff_timer+=1
            logging.debug("Delete preparer sleeping for {} sec".format(backoff_timer))
            time.sleep(backoff_timer)
        else:
            if backoff_timer > 0:
                backoff_timer = 0 #reset backoff timer

            # get item from queue
            row_to_prepare_with_tokens = delete_preparer_queue.get()
            row_to_prepare = row_to_prepare_with_tokens.row

            prepared_dictionary = {}
            prepared_dictionary[key] = getattr(row_to_prepare, key)
            prepared_dictionary[extra_key] = getattr(row_to_prepare, extra_key)

            token_min = "token({key},{extra_key}) >= {token_min}".format(key=key, extra_key=extra_key,token_min=row_to_prepare_with_tokens.min)
            token_max = "token({key},{extra_key}) < {token_max}".format(key=key, extra_key=extra_key,token_max=row_to_prepare_with_tokens.max)

            sql = "{sql_statement} where {token_min} and {token_max} and ".format(sql_statement=sql_statement, token_min=token_min, token_max=token_max)

            #
            #
            andcount = 0
            for rkey in prepared_dictionary:
                value = prepared_dictionary[rkey]
                # cassandra is timezone aware, however the response that we would have received
                # previously does not contain timezone, so we need to add it manually
                if isinstance(value, datetime.datetime):
                    value = value.replace(tzinfo=datetime.timezone.utc)
                value = "'{}'".format(value)
                sql += "{key_name} = {qkey}".format(key_name=rkey, qkey=value)
                if andcount < 1:
                    andcount += 1
                    sql += " and "
            delete_queue.put(sql)



def delete_rows(queues, rsettings):
    for row in get_rows(queues, rsettings):
        sql_template = "delete from {keyspace}.{table} where token({key},{extra_key}) >= {min} and token({key},{extra_key}) < {max} and {key} = '{value}' and {extra_key} = '{extra_value}'"
        sql_statement = sql_template.format(keyspace=rsettings.keyspace, table=rsettings.table, key=rsettings.key, extra_key=rsettings.extra_key, min=row.min, max=row.max, value=row.value.get(rsettings.key), extra_value=utc_time(row.value.get(rsettings.extra_key)))
        t = CassandraWorkerTask(sql_statement, (row.min, row.max))
        t.task_type = "delete" # used for statistics purpose only
        queues.worker_queue.put(t)
        queues.stats_queue_delete_scheduled.put(0)


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


def get_rows(queues, rsettings):
    """Generator that returns rows as we get them from worker"""

    sql_template = "select * from {keyspace}.{table}"
    sql_statement = sql_template.format(keyspace=rsettings.keyspace, table=rsettings.table)
    mt = Mapper_task(sql_statement, rsettings.key, rsettings.filter_string)
    mt.parser = get_result_parser
    queues.mapper_queue.put(mt)
    while True:
        if queues.results_queue.empty():
            logging.debug("Waiting on results...")
            time.sleep(5)
        else:
            yield queues.results_queue.get()
            queues.stats_queue_results_consumed.put(0)





def get_rows_count(queues, rsettings):


    sql_template = "select count(*) from {keyspace}.{table}"

    sql_statement = sql_template.format(keyspace=rsettings.keyspace, table=rsettings.table)
    count = 0
    aggregate = True
    mt = Mapper_task(sql_statement, rsettings.key, rsettings.filter_string)
    mt.parser = count_result_parser;
    queues.mapper_queue.put(mt)
    total = 0
    while True:
        if queues.results_queue.empty():
            logging.debug("Waiting on results...")
            logging.debug("Total so far: {}".format(total))
            time.sleep(5)
        else:
            res = queues.results_queue.get()
            if res is False:
                # kill pill received
                # end the loop and present the results
                break
            queues.stats_queue_results_consumed.put(0)
            total += res.value
    # send kill signal to process manager to stop all workers
    queues.kill.set()
    time.sleep(4) # wait for the kill event to reach all processes
    return total

    # now, chill and wait for results
    #
    #
    #  this was needed for wide partition finder, the count per partition
    #
    #
    # unaggregated_count = []
    # while result.qsize() > 0:
    #     r = result.get()
    #     if aggregate:
    #         count += r.value[0].count
    #     else:
    #         split_count = Result(r.min, r.max, r.value[0])
    #         unaggregated_count.append(split_count)
    # if aggregate:
    #     return count
    # else:
    #     return unaggregated_count


def print_rows(queues, rsettings):
    for row in get_rows(queues, rsettings):
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


def print_rows_count(queues, rsettings):
    count = get_rows_count(queues, rsettings)
    print("Total amount of rows in {keyspace}.{table} is {count}".format(
        keyspace=rsettings.keyspace, table=rsettings.table, count=count))

def stats_monitor(queues, rsettings):
    start_time = datetime.datetime.now()
    stats_split_count = 0
    stats_split_count_delta = 0
    stats_map_count = 0
    stats_deleted_count = 0
    stats_delete_scheduled_count = 0
    stats_map_count_delta = 0
    stats_worker_count = 0
    stats_result_count = 0
    stats_result_count_delta = 0
    stats_result_consumed_count = 0
    stats_result_consumed_count_delta = 0



    predicted_split_count = round(split_predicter(rsettings.tr,rsettings.split))
    last_iteration_time = None
    overall_start_time = datetime.datetime.now()
    while not queues.kill.is_set():
        iteration_start = datetime.datetime.now()
        # TODO: refactor this to use a map of queue and stats counter and do it in a loop instead of 3 conditionals
        if not queues.stats_queue_splits.empty():
            try:
                stats_split_count_prev = stats_split_count
                while not queues.stats_queue_splits.empty():
                    queues.stats_queue_splits.get()
                    stats_split_count += 1
                stats_split_count_delta = stats_split_count - stats_split_count_prev

            except:
                logging.warning("Stats queue for splits empty, but noone else should have been consuming it.")

        if not queues.stats_queue_mapper.empty():
            try:
                stats_map_count_prev = stats_map_count
                while not queues.stats_queue_mapper.empty():
                    queues.stats_queue_mapper.get()
                    stats_map_count += 1
                stats_map_count_delta = stats_map_count - stats_map_count_prev
            except:
                logging.warning("Stats queue for mapper empty, but noone else should have been consuming it.")

        if not queues.stats_queue_results.empty():
            try:
                stats_result_count_prev = stats_result_count
                while not queues.stats_queue_results.empty():
                    queues.stats_queue_results.get()
                    stats_result_count += 1
                stats_result_count_delta = stats_result_count - stats_result_count_prev
            except:
                logging.warning("Stats queue for results empty, but noone else should have been consuming it.")
        if not queues.stats_queue_results_consumed.empty():
            try:
                while not queues.stats_queue_results_consumed.empty():
                    queues.stats_queue_results_consumed.get()
                    stats_result_consumed_count += 1
            except:
                logging.warning("Stats queue for results_consumed empty, but noone else should have been consuming it.")

        if not queues.stats_queue_deleted.empty():
            try:
                while not queues.stats_queue_deleted.empty():
                    queues.stats_queue_deleted.get()
                    stats_deleted_count += 1
            except:
                logging.warning("Stats queue for deleted empty, but noone else should have been consuming it.")

        if not queues.stats_queue_delete_scheduled.empty():
            try:
                while not queues.stats_queue_delete_scheduled.empty():
                    queues.stats_queue_delete_scheduled.get()
                    stats_delete_scheduled_count += 1
            except:
                logging.warning("Stats queue for delete scheduled empty, but noone else should have been consuming it.")

        if last_iteration_time:
            iteration_delta = iteration_start - last_iteration_time
            result_rate = round(stats_result_count_delta / iteration_delta.total_seconds())
            results_remaining = predicted_split_count - stats_result_count
            if result_rate == 0:
                continue
            seconds_remaining = results_remaining / result_rate
            percent = predicted_split_count / 100
            done_percent = round(stats_result_count / percent)
            print("result delta delta is {}".format(stats_result_count_delta))
            print("Iteration delta is {}".format(iteration_delta.total_seconds()))
            print()
            print("Performance::  splits: {}/{} ({}), maps: {} ({}), results consumption: {}/{} ({})".format(
                stats_split_count, predicted_split_count, stats_split_count_delta, stats_map_count,
                stats_map_count_delta, stats_result_consumed_count, stats_result_count, stats_result_count_delta))
            print("{}% done. {} results/s. Seconds remaining: {}".format(done_percent, result_rate, seconds_remaining))
        if stats_delete_scheduled_count >0:
            print("Deleted {}/{} rows".format(stats_deleted_count,stats_delete_scheduled_count))
        time.sleep(1)
        last_iteration_time = iteration_start
    else:
        logging.debug("Stats monitor exiting.")

def queue_monitor(queues, rsettings):
    while not queues.kill.is_set():

        logging.debug("Queue status:")
        logging.debug("Split queue full: {} empty: {}".format(queues.split_queue.full(), queues.split_queue.empty()))
        logging.debug("Map queue full: {} empty: {}".format(queues.mapper_queue.full(), queues.mapper_queue.empty()))
        logging.debug("Worker queue full: {} empty: {}".format(queues.worker_queue.full(), queues.worker_queue.empty()))
        logging.debug("Results queue full: {} empty: {}".format(queues.results_queue.full(), queues.results_queue.empty()))
        time.sleep(5)
    else:
        logging.debug("Queue monitor exiting.")

def process_manager(queues, rsettings):

    # queue monitor
    qmon_process = multiprocessing.Process(target=queue_monitor, args=(queues, rsettings))
    qmon_process.start()

    # stats monitor
    smon_process = multiprocessing.Process(target=stats_monitor, args=(queues, rsettings))
    smon_process.start()

    # start splitter
    splitter_process = multiprocessing.Process(target=splitter, args=(queues, rsettings))
    splitter_process.start()

    # mapper
    mapper_process = multiprocessing.Process(target=mapper, args=(queues,rsettings))
    mapper_process.start()

    # TODO: remove this, as reducer is not used
    # reducer
    #reducer_process = multiprocessing.Process(target=reducer, args=(queues,rsettings))
    #reducer_process.start()

    workers = []
    for w in range(rsettings.workers):
        # workers
        worker_process = multiprocessing.Process(target=cassandra_worker, args=(queues,rsettings))
        worker_process.start()
        workers.append(worker_process)

    while not queues.kill.is_set():
        for w in workers:
            if not w.is_alive():
                logging.warning("Process {} died.".format(w))
                workers.remove(w)
                time.sleep(1)
                logging.warning("Starting a new process")
                worker_process = multiprocessing.Process(target=cassandra_worker, args=(queues, rsettings))
                worker_process.start()
                workers.append(worker_process)
        time.sleep(1)
    else:
        logging.debug("Global kill event! Process manager is stopping.")

def reducer2(queues, rsettings):
    """Filter out the relevant information from Cassandra results"""

    pid = os.getpid()
    print("Reducer started")
    while True:
        # wait for work
        if queues.reducer_queue.empty():
            logging.debug("Reducer {} waiting for work".format(pid))
            time.sleep(2)
        else:
            result = queues.reducer_queue.get()
            logging.debug("Got task {} from reducer queue".format(result))
            for row in result.value:
                queues.results_queue.put(row)

def utc_time(value):
    if isinstance(value, datetime.datetime):
        value = value.replace(tzinfo=datetime.timezone.utc)
    return value

def count_result_parser(row, rsettings=None):
    return row.count

def get_result_parser(row, rsettings=None):
    results_that_we_care_about = {}
    results_that_we_care_about[rsettings.key] = getattr(row, rsettings.key)
    results_that_we_care_about[rsettings.extra_key] = getattr(row, rsettings.extra_key)

    return results_that_we_care_about

def cassandra_worker(queues, rsettings):
    """Executes SQL statements and puts results in result queue"""
    cas_settings = rsettings.cas_settings
    pid = os.getpid()
    if "," in cas_settings.host:
        host = random.choice(cas_settings.host.split(","))
        logging.info("Picking random host: {}".format(host))
    else:
        host = cas_settings.host
    # starting bunch of sessions at the same time might not be idea, so we add
    # a bit of random delay
    if rsettings.worker_max_delay_on_startup > 0:
        time.sleep(random.choice(range(rsettings.worker_max_delay_on_startup)))
    session = get_cassandra_session(host, cas_settings.port, cas_settings.user,
                                    cas_settings.password, cas_settings.ssl_cert, cas_settings.ssl_key, cas_settings.dc, cas_settings.cacert,
                                    cas_settings.ssl_v1 )

    sql = "use {}".format(rsettings.keyspace)
    logging.debug("Executing SQL: {}".format(sql))
    session.execute(sql)
    if not session.is_shutdown:
        logging.debug("Worker {} connected to Cassandra.".format(pid))
        while not queues.kill.is_set():
            # wait for work
            if queues.worker_queue.empty():
                logging.debug("Worker {} waiting for work".format(pid))
                time.sleep(2)
            else:
                task = queues.worker_queue.get()
                if task is False:
                    # kill pill received
                    # pass it to the results queue
                    queues.results_queue.put(False)
                    continue # and return back to waiting for work
                logging.debug("Got task {} from worker queue".format(task))
                try:
                    r = session.execute(task.sql)
                except:
                    logging.warning("Cassandra connection issues!")
                    return False
                if task.task_type == "delete":
                    queues.stats_queue_deleted.put(0)
                    logging.debug("DELETE: {}".format(task.sql))
                else:
                    for row in r:
                        logging.debug(row)
                        if task.parser:
                            row = task.parser(row, rsettings)
                        res = Result(task.split_min, task.split_max, row)
                        logging.debug(res)
                        queues.results_queue.put(res)
                        queues.stats_queue_results.put(0)
        else:
            logging.debug("Worker stopping due to kill event.")


def mapper(queues, rsettings):
    """Prepares SQL statements for worker and puts tasks in worker queue"""
    try:
        map_task = queues.mapper_queue.get(True,10) # initially, wait for 5 sec to receive first work orders
    except:
        logging.warning("Mapper did not receive any work...timed out.")
        return False

    print("mapper Received work assignment::: {}".format(map_task.sql_statement))

    while True:
        if queues.split_queue.empty():
            logging.debug("Split queue empty. Mapper is waiting")
            time.sleep(1)
        else:
            split = queues.split_queue.get()
            if split is False:
                # this is a kill pill, no more work, let's relax
                logging.debug("Mapper has received kill pill, passing it on to workers and exiting.")
                queues.worker_queue.put(False) # pass the kill pill
                return True

            if rsettings.extra_key:
                sql = "{statement} where token({key}, {extra_key}) >= {min} and token({key}, {extra_key}) < {max}".format(statement=map_task.sql_statement, key=map_task.key_column, extra_key=rsettings.extra_key, min=split[0], max=split[1])
            else:
                sql = "{statement} where token({key}) >= {min} and token({key}) < {max}".format(statement=map_task.sql_statement, key=map_task.key_column, min=split[0], max=split[1])

            if rsettings.filter_string:
                sql = "{} and {}".format(sql, rsettings.filter_string)
            t = CassandraWorkerTask(sql, split, map_task.parser)
            queues.worker_queue.put(t)
            queues.stats_queue_mapper.put(0)
            logging.debug("Mapper prepared work task: {}".format(sql))



if __name__ == "__main__":

    py_version = platform.python_version_tuple()
    if int(py_version[0]) < 3:
        logging.info("Python 3.6 or newer required. 3.7 recommended.")
        sys.exit(1)

    args = parse_user_args()

    if args.debug:
        logging.basicConfig(level=logging.DEBUG)
        logging.debug('Logging started.')
    else:
        logging.basicConfig(level=logging.INFO)

    # TODO: move this to runtime settings constructor
    if args.min_token and args.max_token:
        tr = Token_range(args.min_token, args.max_token)
    else:
        tr = Token_range(default_min_token, default_max_token)

    cas_settings = CassandraSettings()
    cas_settings.host = args.host
    cas_settings.user = args.user
    cas_settings.port = args.port
    cas_settings.password = args.password
    cas_settings.ssl_cert = args.ssl_cert
    cas_settings.ssl_key = args.ssl_key
    cas_settings.ssl_v1 = args.ssl_v1
    cas_settings.cacert = args.cacert
    cas_settings.dc = args.datacenter

    queues = Queues()

    rsettings = RuntimeSettings()
    rsettings.keyspace = args.keyspace
    rsettings.table = args.table
    rsettings.split = args.split
    rsettings.key = args.key
    rsettings.extra_key = args.extra_key
    rsettings.filter_string = args.filter_string
    rsettings.tr = tr
    rsettings.cas_settings = cas_settings
    rsettings.workers = args.workers
    if rsettings.workers > 10:
        # if more than 10 workers are used, we add delay to their startup logic
        rsettings.worker_max_delay_on_startup = rsettings.workers * 2

    pm = multiprocessing.Process(target=process_manager, args=(queues, rsettings))
    pm.start()

    # TODO: needs re-implementation
    if args.action == "find-nulls":
        find_null_cells(args.keyspace, args.table, "id", "comment")
    elif args.action == "count-rows":
        print_rows_count(queues, rsettings)
    elif args.action == "print-rows":
        print_rows(queues, rsettings)
    elif args.action == "delete-rows":
        delete_rows(queues, rsettings)
    # TODO: needs re-implementation
    elif args.action == "find-wide-partitions":
        find_wide_partitions(args.keyspace, args.table, args.key,
                             args.split, args.value_column, args.filter_string)
    # TODO: needs re-implementation
    elif args.action == "update-rows":
        update_rows(args.keyspace, args.table, args.key,
                    args.update_key, args.update_value, args.split,
                    args.filter_string, args.extra_key)
    else:
        # this won't be accepted by argparse anyways
        sys.exit(1)
