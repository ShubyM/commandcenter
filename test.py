from datetime import datetime
from pydantic import BaseModel

from commandcenter.sources.pi_web import PISubscription
from commandcenter.sources.traxx import TraxxSubscription
from commandcenter.timeseries import UnitOp



unitop = UnitOp(
    unitop_id="test-unitop3",
    data_mapping={
        "flow": PISubscription(
            web_id="F1DPEmoryo_bV0GzilxLXH31pgIYIAAAQUJDX1BJX09QU1xGSUM2ODAwNDAuUFY",
            name="FIC680040.PV",
        ),
        "prox": TraxxSubscription(
            asset_id=3863,
            sensor_id="AEYYYQ3ERWPLUA6LJFBMEOQFOE"
        )
    },
    meta={"BU": 2, "Suite": 4, "SuiteDesignation": "Ferm"}
)
print(unitop.json(indent=4))


class WatchdogEvent(BaseModel):
    name: str
    unitop_id: str
    state: str
    timestamp: datetime


print(WatchdogEvent.schema_json(indent=4))