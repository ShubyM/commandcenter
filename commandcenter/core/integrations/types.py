from datetime import datetime
from typing import Dict, List, Tuple, Type, Union


JSONPrimitive = Union[str, int, float, bool, Type[None]]
JSONContent = Union[JSONPrimitive, List["JSONContent"], Dict[str, "JSONContent"]]
TimeseriesRow = Tuple[datetime, List[JSONPrimitive]]