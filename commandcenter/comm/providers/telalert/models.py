from typing import Any, Dict, Sequence

from pydantic import BaseModel, root_validator



class TelAlertMessage(BaseModel):
    """Model for a TelAlert message."""
    msg: str
    groups: Sequence[str] | None
    destinations: Sequence[str] | None
    subject: str | None
    
    @root_validator
    def _vlaidate_message(cls, v: Dict[str, Any]) -> Dict[str, Any]:
        """Ensure that at least 1 of 'groups' or 'destinations' is present."""
        groups = v["groups"]
        destinations = v["destinations"]
        if not groups and not destinations:
            raise ValueError("Must provide either a group or destination to deliver message")
        return v