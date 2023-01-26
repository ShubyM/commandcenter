from uplink import Consumer, Query, headers, get



@headers({"Accept": "application/json"})
class Streams(Consumer):
    """https://docs.aveva.com/bundle/pi-web-api-reference/page/help/controllers/stream.html"""

    @get("/piwebapi/streams/{web_id}/end")
    def get_end(
        self,
        web_id: str,
        desiredUnits: Query = None,
        selectedFields: Query = None
    ):
        """https://docs.aveva.com/bundle/pi-web-api-reference/page/help/controllers/stream/actions/getend.html"""
    
    @get("/piwebapi/streams/{web_id}/interpolated")
    def get_interpolated(
        self,
        web_id: str,
        startTime: Query = None,
        endTime: Query = None,
        timeZone: Query = None,
        interval: Query = None,
        syncTime: Query = None,
        syncTimeBoundaryType: Query = None,
        desiredUnits: Query = None,
        filterExpression: Query = None,
        includeFilteredValues: Query = None,
        selectedFields: Query = None
    ):
        """https://docs.aveva.com/bundle/pi-web-api-reference/page/help/controllers/stream/actions/getinterpolated.html"""

    @get("/piwebapi/streams/{web_id}/recorded")
    def get_recorded(
        self,
        web_id: str,
        startTime: Query = None,
        endTime: Query = None,
        timeZone: Query = None,
        boundaryType: Query = None,
        desiredUnits: Query = None,
        filterExpression: Query = None,
        includeFilteredValues: Query = None,
        maxCount: Query = None,
        selectedFields: Query = None,
        associations: Query = None
    ):
        """https://docs.aveva.com/bundle/pi-web-api-reference/page/help/controllers/stream/actions/getrecorded.html"""

    @get("/piwebapi/streams/{web_id}/recordedattime")
    def get_recorded_at_time(
        self,
        web_id: str,
        time: Query = None,
        timeZone: Query = None,
        retrievalMode: Query = None,
        desiredUnits: Query = None,
        selectedFields: Query = None,
        associations: Query = None
    ):
        """https://docs.aveva.com/bundle/pi-web-api-reference/page/help/controllers/stream/actions/getrecordedattime.html"""