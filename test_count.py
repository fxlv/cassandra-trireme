import count


def test_seconds_to_human():
    assert count.seconds_to_human(60) == (0,1,0)
    assert count.seconds_to_human(715) == (0,11,55)
    assert count.seconds_to_human(52812) == (14,40,12)

def test_human_time():
    assert count.human_time(60) == "{} hours, {} minutes, {} seconds".format(0,1,0)
    assert count.human_time(1562) == "{} hours, {} minutes, {} seconds".format(0,26,2)
    assert count.human_time(2512) == "{} hours, {} minutes, {} seconds".format(0,41,52)
    assert count.human_time(13749) == "{} hours, {} minutes, {} seconds".format(3,49,9)