import count
import presentation


def test_seconds_to_human():
    assert presentation.seconds_to_human(60) == (0, 1, 0)
    assert presentation.seconds_to_human(715) == (0, 11, 55)
    assert presentation.seconds_to_human(52812) == (14, 40, 12)

def test_human_time():
    assert presentation.human_time(60) == "{} hours, {} minutes, {} seconds".format(0, 1, 0)
    assert presentation.human_time(1562) == "{} hours, {} minutes, {} seconds".format(0, 26, 2)
    assert presentation.human_time(2512) == "{} hours, {} minutes, {} seconds".format(0, 41, 52)
    assert presentation.human_time(13749) == "{} hours, {} minutes, {} seconds".format(3, 49, 9)