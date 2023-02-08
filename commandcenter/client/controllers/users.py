from uplink import Body, Consumer, headers, get, post


@headers({"Accept": "application/json"})
class Users(Consumer):

    @get("/users/whoami")
    def whoami(self):
        """Retrieve user information for current logged in user."""

    
    @post("/users/token")
    def token(self, **oauth_args: Body):
        """Retrieve an access token for the API."""