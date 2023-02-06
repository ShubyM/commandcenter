from commandcenter.api.setup.timeseries import setup_timeseries_handler
from commandcenter.timeseries import MongoTimeseriesHandler



def get_timeseries_handler() -> MongoTimeseriesHandler:
    """Dependency for retrieving a timeseries handler."""
    return setup_timeseries_handler()