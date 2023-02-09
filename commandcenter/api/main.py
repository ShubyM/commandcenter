from commandcenter.api.routes import (
    events_,
    pi_web_,
    telalert_,
    traxx_,
    unitop_,
    users_
)
from commandcenter.api.setup.application import setup_application



app = setup_application(
    title="commandcenter",
    description="The hub for real-time data integration.",
    version="0.0.1"
)

app.include_router(events_)
app.include_router(pi_web_)
app.include_router(telalert_)
app.include_router(traxx_)
app.include_router(unitop_)
app.include_router(users_)