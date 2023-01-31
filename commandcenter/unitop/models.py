from datetime import datetime
from typing import Any, Dict

from beanie import Document, Indexed, Insert, Replace, Update, before_event
from pydantic import Field

from commandcenter.integrations.models import BaseSubscription
from commandcenter.context import user_context



class Unitop(Document):
    name: Indexed(str, unique=True)
    unitopType: str = Field(alias="unitop_type")
    fieldMappings: Dict[str, BaseSubscription] = Field(alias="field_mappings")
    meta: Dict[str, Any]
    lastModifiedDate: datetime = Field(default=None, exclude=True)
    lastModifiedBy: str = Field(default=None, exclude=True)

    @before_event(Insert, Replace, Update)
    def last_modified(self) -> None:
        user = user_context.get()
        if user is None:
            raise RuntimeError("User context not set. Cannot modify database.")
        self.lastModifiedBy = user.identity
        self.lastModifiedDate = datetime.now()

    class Settings:
        name="unitops"