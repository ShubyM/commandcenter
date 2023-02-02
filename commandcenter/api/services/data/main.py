from commandcenter.services.data.routes import pi_web_, traxx_, unitop_
from commandcenter.setup.application.application import setup_application



app = setup_application(
    title="Data Integrations API",
    description="commandcenter data integration layer.",
    version="0.0.1"
)
app.include_router(pi_web_)
app.include_router(traxx_)
app.include_router(unitop_)