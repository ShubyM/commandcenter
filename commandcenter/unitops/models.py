from typing import Any, Dict

from pydantic import BaseModel

from commandcenter.integrations.models import AnySubscription



class UnitOp(BaseModel):
    """Model for a unit operation. A unit op is a logical grouping of labeled
    subscriptions with associated meta data.
    """
    unitop_id: str
    data_mapping: Dict[str, AnySubscription]
    meta: Dict[str, Any] | None