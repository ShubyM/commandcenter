from datetime import datetime
from typing import Dict, List, Tuple, Type



JSONPrimitive = str | int | float | bool | Type[None]
JSONContent = JSONPrimitive | List["JSONContent"] | Dict[str, "JSONContent"]

TimeseriesRow = Tuple[datetime, List[JSONPrimitive]]