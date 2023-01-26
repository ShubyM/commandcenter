from commandcenter.sources.pi_web import PISubscription, PIWebClient
from commandcenter.http.aiohttp import create_auth_handlers, NegotiateAuth

from aiohttp import ClientSession


async def receive(client: PIWebClient):
    async for msg in client.messages():
        print(msg)



subscriptions = [
    PISubscription(
        web_id="F1DPEmoryo_bV0GzilxLXH31pgIYIAAAQUJDX1BJX09QU1xGSUM2ODAwNDAuUFY",
        name="FIC680040.PV",
    ),
    PISubscription(
        web_id="F1DPEmoryo_bV0GzilxLXH31pgeYIAAAQUJDX1BJX09QU1xBSTY4MDAxNUEuUFY",
        name="AI680015A.PV"
    ),
    PISubscription(
        web_id="F1DPEmoryo_bV0GzilxLXH31pgeoIAAAQUJDX1BJX09QU1xBSTY4MDAxNUIuUFY",
        name="AI680015B.PV"
    )
]


async def main():
    req, rep = create_auth_handlers(NegotiateAuth())
    session = 1