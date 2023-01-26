from uplink import Consumer, Query, headers, get



@headers({"Accept": "application/json"})
class Points(Consumer):
    """https://docs.aveva.com/bundle/pi-web-api-reference/page/help/controllers/point.html"""

    @get("/piwebapi/points/{web_id}")
    def get(
        self,
        web_id: str,
        selectedFields: Query = None,
        webIdType: Query = None
    ):
        """https://docs.aveva.com/bundle/pi-web-api-reference/page/help/controllers/point/actions/get.html"""