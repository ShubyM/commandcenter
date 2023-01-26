from commandcenter.services.comm.routes import telalert_
from commandcenter.setup.application.application import setup_application



app = setup_application(
    title="Communucations API",
    description="commandcenter communications integration layer.",
    version="0.0.1"
)
app.include_router(telalert_)