from typing import Any, Dict, Optional, Sequence

from pydantic import BaseModel, root_validator



class TelAlertMessage(BaseModel):
    """Validated model for a TelAlert message."""
    msg: str
    groups: Optional[Sequence[str]] = None
    destinations: Optional[Sequence[str]] = None
    subject: Optional[str] = None
    
    @root_validator
    def _vlaidate_message(cls, v: Dict[str, Any]) -> Dict[str, Any]:
        """Validate that at least 1 of 'groups' or 'destinations' is present."""
        groups = v["groups"]
        destinations = v["destinations"]
        if not groups and not destinations:
            raise ValueError("Must provide either a group or destination to deliver message")
        return v