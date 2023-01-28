from commandcenter.sources.pi_web import PISubscription, PIWebClient
from commandcenter.http.aiohttp import create_auth_handlers, NegotiateAuth
from commandcenter.integrations.models import DroppedSubscriptions
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

subscriptions_2 = [
    PISubscription(
        web_id="F1DPEmoryo_bV0GzilxLXH31pgk4IAAAQUJDX1BJX09QU1xBSTY4MDA1OUEuUFY",
        name="AI680059A.PV",
    ),
    PISubscription(
        web_id="F1DPEmoryo_bV0GzilxLXH31pglIIAAAQUJDX1BJX09QU1xBSTY4MDA1OUIuUFY",
        name="AI680059B.PV"
    ),
    PISubscription(
        web_id="F1DPEmoryo_bV0GzilxLXH31pgl4IAAAQUJDX1BJX09QU1xBSUM2ODAwNTkuUFY",
        name="AIC680059.PV"
    )
]


async def test_lock():
    pass

async def main():
    req, rep = create_auth_handlers(NegotiateAuth())
    session = ClientSession(
        base_url="wss://pivisionabc.abbvienet.com",
        request_class=req,
        response_class=rep
    )
    client = PIWebClient(
        session=session
    )
    client.subscribe(set(subscriptions))
    client.subscribe(set(subscriptions_2))
    try:
        await receive(client)
    finally:
        await client.close()


if __name__ == "__main__":
    import logging
    import asyncio
    logging.basicConfig(level=logging.DEBUG)
    asyncio.run(main())