import json
from datetime import datetime
from pydantic import BaseModel

from commandcenter.sources.pi_web import PISubscription
from commandcenter.sources.traxx import TraxxSubscription
from commandcenter.timeseries import UnitOp

Z9819 = '{"CO2_Concentration": {"Tag": "Z9819C02WL.PV", "WebId": "F1DPEmoryo_bV0GzilxLXH31pghWAAAAQUJDX1BJX09QU1xaOTgxOUMwMldMLlBW"}, "Gas_Flow": {"Tag": "Z9819GAS1WL.PV", "WebId": "F1DPEmoryo_bV0GzilxLXH31pghmAAAAQUJDX1BJX09QU1xaOTgxOUdBUzFXTC5QVg"}, "Temp_Probe_A": {"Tag": "Z9819TEMP1WL.PV", "WebId": "F1DPEmoryo_bV0GzilxLXH31pgh2AAAAQUJDX1BJX09QU1xaOTgxOVRFTVAxV0wuUFY"}, "Temp_Probe_B": {"Tag": "Z9819TEMP2WL.PV", "WebId": "F1DPEmoryo_bV0GzilxLXH31pgiGAAAAQUJDX1BJX09QU1xaOTgxOVRFTVAyV0wuUFY"}}'
data_mapping = json.loads(Z9819)

data_mapping_sub = {
    k: PISubscription(name=v["Tag"], web_id=v["WebId"]) for k, v in data_mapping.items()
}
data_mapping_sub["Rocking_Confirmation"] = TraxxSubscription(
            asset_id=3863,
            sensor_id="AEYYYQ3ERWPLUA6LJFBMEOQFOE"
        )

unitop = UnitOp(
    unitop_id="Z9819",
    data_mapping=data_mapping_sub,
    meta={"Class": "Rocker"}
)
print(unitop.json(indent=4))

# unitop = UnitOp(
#     unitop_id="test-unitop3",
#     data_mapping={
#         "flow": PISubscription(
#             web_id="F1DPEmoryo_bV0GzilxLXH31pgIYIAAAQUJDX1BJX09QU1xGSUM2ODAwNDAuUFY",
#             name="FIC680040.PV",
#         ),
#         "prox": TraxxSubscription(
#             asset_id=3863,
#             sensor_id="AEYYYQ3ERWPLUA6LJFBMEOQFOE"
#         )
#     },
#     meta={"BU": 2, "Suite": 4, "SuiteDesignation": "Ferm"}
# )
# print(unitop.json(indent=4))


class WatchdogEvent(BaseModel):
    name: str
    unitop_id: str
    unitop_state: str
    watchdog_state: str
    timestamp: datetime


print(WatchdogEvent.schema_json(indent=4))

print(WatchdogEvent(name="rocking_confirmation", unitop_id="Z9819", unitop_state="RUNNING", watchdog_state="GOOD", timestamp=datetime.now()).json())