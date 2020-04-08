import multiprocessing
import queue

from settings import split_q_size, worker_q_size, mapper_q_size, reducer_q_size, results_q_size, stats_q_size


class Result:
    def __init__(self, min, max, value):
        self.min = min
        self.max = max
        self.value = value

    def __str__(self):
        return "Result(min: {}, max: {}, value: {})".format(
            self.min, self.max, self.value)


class RowForDeletion:
    def __init__(self, min, max, row):
        self.min = min
        self.max = max
        self.row = row

    def __str__(self):
        return "Row for deletion: (min: {}, max: {}, row: {})".format(
            self.min, self.max, self.row)


class Settings:
    def __init__(self):
        pass


class Token_range:
    def __init__(self, min, max):
        self.min = min
        self.max = max


class Mapper_task:
    def __init__(self, sql_statement, key_column, filter_string):
        self.sql_statement = sql_statement
        self.key_column = key_column
        self.filter_string = filter_string
        self.parser = None

    def __str__(self):
        return "Mapper task: {}".format(self.sql_statement)


class Queues:

    def __init__(self):
        self.split_queue = multiprocessing.Queue(split_q_size)
        self.worker_queue = multiprocessing.Queue(worker_q_size)
        self.mapper_queue = multiprocessing.Queue(mapper_q_size)
        self.reducer_queue = multiprocessing.Queue(reducer_q_size)
        self.results_queue = multiprocessing.Queue(results_q_size)

        # stats queues are used to count events and calculate performance metrics
        self.stats_queue_splits = multiprocessing.Queue(stats_q_size)
        self.stats_queue_mapper = multiprocessing.Queue(stats_q_size)
        self.stats_queue_worker = multiprocessing.Queue(stats_q_size)
        self.stats_queue_results = multiprocessing.Queue(stats_q_size)
        self.stats_queue_deleted = multiprocessing.Queue(stats_q_size)
        self.stats_queue_delete_scheduled = multiprocessing.Queue(stats_q_size)
        self.stats_queue_results_consumed = multiprocessing.Queue(stats_q_size)
        # kill event, while it is not a queue, we'd love to pass it around
        self.kill = multiprocessing.Event()


class RuntimeSettings:
    """Object for passing around settings for runtime use"""
    def __init__(self):
        self.keyspace = None
        self.table = None
        self.split = None
        self.key = None
        self.extra_key = None
        self.value_column = None
        self.filter_string = None
        self.tr = None
        self.cas_settings = None
        self.worker_max_delay_on_startup = 0


class CassandraSettings:

    def __init__(self):
        self.host = None
        self.port = None
        self.user = None
        self.password = None
        self.ssl_cert = None
        self.ssl_key = None
        self.ssl_v1 = None


class CassandraWorkerTask:
    def __init__(self, sql, split, parser=None):
        self.sql = sql
        self.parser = parser
        self.split_min = split[0]
        self.split_max = split[1]
        self.task_type = "select"

    def __str__(self):
        return "CassandraWorkerTask: {}".format(self.sql)