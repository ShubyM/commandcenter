from uplink import Body, Consumer, Query, headers, get, json, post


@headers({"Accept": "application/json"})
class UnitOp(Consumer):

    @get("/unitop/search/{unitop_id}")
    def unitop(self, unitop_id: str):
        """Retrieve a unitop record."""

    @get("/unitop/search")
    def unitops(self, q: Query):
        """Retrieve a collection of unitop records."""

    @json
    @post("/unitop/save")
    def save(self, **unitop: Body):
        """Save a unitop to the database."""