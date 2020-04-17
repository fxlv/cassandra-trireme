import datetime
import logging
import time

from trireme.presentation import human_time


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


    sleep_time = 1 # default sleep time
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
            # avoid division by zero in the very beginning,
            # when not enough work is done yet
            if percent == 0:
                done_percent = 0
            else:
                done_percent = round(stats_result_count / percent)


            print()
            print("Performance::  splits: {}/{} ({}), maps: {} ({}), results consumption: {}/{} ({})".format(
                stats_split_count, predicted_split_count, stats_split_count_delta, stats_map_count,
                stats_map_count_delta, stats_result_consumed_count, stats_result_count, stats_result_count_delta))
            print("{}% done. {} results/s. Time remaining: {}".format(done_percent, result_rate, human_time(seconds_remaining)))
            if stats_delete_scheduled_count >0:
                print("Deleted {}/{} rows".format(stats_deleted_count,stats_delete_scheduled_count))
            # how often we print updates depends on how much time the
            # script execution is expected to take
            if seconds_remaining > 120:
                sleep_time = 10
            elif seconds_remaining > 60:
                sleep_time = 5
            else: sleep_time = 2
        time.sleep(sleep_time)
        last_iteration_time = iteration_start
    else:
        logging.debug("Stats monitor exiting.")


def split_predicter(tr, split):
    # how many splits will there be?
    predicted_split_count = (tr.max - tr.min) / pow(10, split)
    return predicted_split_count