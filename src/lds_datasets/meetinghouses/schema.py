"""Meetinghouse Schema."""
from datetime import datetime

from pydantic import BaseModel


class Identifiers(BaseModel):
    """Identifiers Schema."""

    facilityId: str
    structureId: str
    propertyId: int
    unitNumber: int | None
    ordId: int | None


class Address(BaseModel):
    """Address Schema."""

    street1: str
    city: str
    county: str
    state: str
    stateId: int
    stateCode: str
    postalCode: str
    country: str
    countryId: int
    countryCode2: str
    countryCode3: str
    formatted: str
    lines: list[str]


class Size(BaseModel):
    """Property Size Schema."""

    value: int
    property_type: str
    display: str


class Geocode(BaseModel):
    """Geocodes Schema."""

    code: str
    type: str


class Language(BaseModel):
    """Language Schema."""

    id: int
    code: str
    display: str


class Associated(BaseModel):
    """Associated Schema."""

    id: str
    associated_type: str
    identifiers: Identifiers
    name: str
    nameDisplay: str
    typeDisplay: str
    # hours
    language: Language


class Building(BaseModel):
    """Building Schema."""

    id: str
    building_type: str
    identifiers: Identifiers
    name: str
    nameDisplay: str
    typeDisplay: str
    address: Address
    # hours: ...
    # timeZone: ...
    propertySize: Size
    interiorSize: Size
    parkingOnsite: int
    parkingOffsite: int
    specialized: bool
    coordinates: list[float]
    coordinatesUpdated: datetime
    geocodes: list[Geocode]
    associated: list[Associated]
    # match
    provider: str
    created: datetime
    updated: datetime
