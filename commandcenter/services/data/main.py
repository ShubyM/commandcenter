from commandcenter.common.application import setup_application
from commandcenter.services.data.routes import pi_web_, traxx_



app = setup_application(
    title="Data API",
    description="CommandCenter data integration layer.",
    version="0.0.1"
)
app.include_router(pi_web_)
app.include_router(traxx_)