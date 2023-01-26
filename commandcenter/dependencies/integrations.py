from commandcenter.integrations.protocols import Manager
from commandcenter.setup.integrations.managers import setup_manager



async def get_manager() -> Manager:
    """Dependency for retrieving a manager based on source context."""
    return setup_manager()