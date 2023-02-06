from commandcenter.api.setup.events import setup_event_bus, setup_event_handler
from commandcenter.events import EventBus, MongoEventHandler



def get_event_handler() -> MongoEventHandler:
    """Dependency for retrieving an event handler."""
    return setup_event_handler()


async def get_event_bus() -> EventBus:
    """Dependency for retrieving an event bus."""
    return setup_event_bus()