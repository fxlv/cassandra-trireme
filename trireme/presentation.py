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