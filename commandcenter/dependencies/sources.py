from typing import List
from fastapi import Depends

from commandcenter.core.sources import AvailableSources
from commandcenter.core.util.context import source_context



class SourceContext:
    """Dependency which sets the data source for a route.
    
    This is critical in ensuring the `get_manager` dependency works correctly.
    """
    def __init__(self, source: AvailableSources):
        self.source = source

    async def __call__(self) -> None:
        print("Source context called.")
        try:
            token = source_context.set(self.source)
            yield
        finally:
            source_context.reset(token)