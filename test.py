import asyncio
import logging
import random
import timeit
from typing import List

from commandcenter.caching.tokens import Tokenable
from commandcenter.caching import memo, singleton




@memo(backend="disk", max_entries=0)
async def test_disk_cache(v: List[int]) -> List[int]:
    await asyncio.sleep(5)
    return v


@memo(backend="redis", max_entries=0)
async def test_redis_cache(v: List[int], _ig: int) -> List[int]:
    await asyncio.sleep(2)
    return v

@singleton
def get(v: List[int]) -> List[int]:
    return v


async def main() -> None:
    memo.set_cache_dir("C:/Users/newvicx/.commandcenter/test")
    l = [random.randint(1, 1000) for _ in range(1000)]
    t = [test_redis_cache(l, random.random()) for _ in range(10)]
    d = [test_disk_cache(l) for _ in range(10)]
    t.extend(d)
    t1 = timeit.default_timer()
    await asyncio.gather(*t)
    t2 = timeit.default_timer()
    print(t2-t1) # Should be close to 5
    verify = await test_redis_cache(l, random.random())
    assert verify == l
    assert id(verify) != l
    verify = await test_disk_cache(l)
    assert verify == l
    assert id(verify) != l
    #await asyncio.sleep(5)
    #memo.set_cache_dir("C:/Users/newvicx/.commandcenter/test")
    #verify = await test_disk_cache(l)
    k = get(l)
    verify = get(l)
    assert id(k) == id(verify)
    await test_redis_cache(5, 5)
    test_disk_cache.clear()
    test_redis_cache.clear()

if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    asyncio.run(main())