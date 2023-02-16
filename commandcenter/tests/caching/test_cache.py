from commandcenter.caching import memo

from time import sleep


@memo
def expensive(num):
    sleep(5)
    return num





def test_saves_time():
    expensive(10)
    expensive(10)
    expensive(10)
    expensive(10)
    expensive(10)
    expensive(10)
    expensive(10)