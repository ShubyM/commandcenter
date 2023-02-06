
from commandcenter.util.http.requests.bearer import OAuth2ResourceOwnerPasswordCredentials
from commandcenter.util.http.aiohttp.client_reqrep import create_auth_handlers
from commandcenter.util.http.aiohttp.flows.bearer import OAuth2ResourceOwnerPasswordCredentials as OAuth2Password

import requests
from aiohttp import ClientSession


def main():
    auth = OAuth2ResourceOwnerPasswordCredentials(
        "http://localhost:8000/token",
        username="johndoe",
        password="password"
    )
    response = requests.get("http://localhost:8000/users/me", auth=auth)
    print(response.json())
    response = requests.get("http://localhost:8000/users/me/items", auth=auth)

async def a_main():
    auth = OAuth2Password(
        "http://localhost:8000/token",
        username="johndoe",
        password="password"
    )
    req, rep = create_auth_handlers(auth)
    session = ClientSession(request_class=req, response_class=rep)
    response = await session.get("http://localhost:8000/users/me/")
    content = await response.json()
    print(content)
    response = await session.get("http://localhost:8000/users/me/items/")
    await response.json()
    await session.close()

# import asyncio
# import csv
# import random
# from datetime import datetime, timedelta
# from typing import List, Dict

# from motor.motor_asyncio import AsyncIOMotorClient

# from commandcenter.timeseries.handler import MongoTimeseriesHandler
# from commandcenter.timeseries.stream import get_timeseries


# START = datetime.now() + timedelta(hours=36)

# def get_sensor_id() -> int:
#     return random.randint(1000, 1010)

# def random_samples(n: int) -> List[Dict[str, datetime | int | float]]:
#     samples = []
#     subscription = get_sensor_id()
#     t = START
#     for _ in range(n):
#         samples.append({"timestamp": t, "value": random.randint(1,100), "subscription": subscription})
#         t = t + timedelta(seconds=random.random()*10)
#     return samples

# def insert_data():
#     handler = MongoTimeseriesHandler()
#     for _ in range(50):
#         samples = random_samples(1000)
#         for sample in samples:
#             handler.send(sample)
#     handler.flush(True)
#     handler.close()


# async def main():
#     client = AsyncIOMotorClient()
#     with open("test_timeseries.csv", "w", newline='') as fh:
#         writer = csv.writer(fh, delimiter=',', quotechar='|', quoting=csv.QUOTE_MINIMAL)
#         async for timestamp, data in get_timeseries(
#             client,
#             {999, 1000, 1001, 1002, 1003, 1004, 1005, 1006, 1011},
#             "commandcenter",
#             start_time=START-timedelta(minutes=30),
#             end_time=START+timedelta(minutes=30)
#         ):
#             row = [timestamp.isoformat(), *data]
#             writer.writerow(row)

if __name__ == "__main__":
    import logging
    import asyncio
    logging.basicConfig(level=logging.DEBUG)
    asyncio.run(a_main())