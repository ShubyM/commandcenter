from commandcenter.setup.integrations.managers import setup_manager
from commandcenter.integrations import Manager



async def get_manager() -> Manager:
    """Dependency for retrieving a manager based on source context."""
    return setup_manager()