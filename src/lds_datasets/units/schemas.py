""""Schemas useful for unit analysis."""
from datetime import datetime
from enum import Enum

from pydantic import BaseModel


class UnitType(Enum):
    """A unit type."""

    stake = "stake"
    district = "district"
    ward = "ward"
    branch = "branch"


class DateRange(BaseModel):
    """A Date Range."""

    start_date: datetime
    end_date: datetime
