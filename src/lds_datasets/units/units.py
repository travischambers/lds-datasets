"""Scrape wards from the meetinghouse locator API."""
import concurrent.futures
import json
import threading
import time
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Literal

import click
import pandas as pd
import requests
import structlog
from matplotlib import pyplot as plt
from pydantic import BaseModel

from lds_datasets.logging import setup_logging

# Get the current timestamp
script_start_time = datetime.now()
script_start_second = script_start_time.strftime("%Y_%m_%d_%H:%M:%S")
script_start_day = script_start_time.strftime("%Y_%m_%d")

yesterday = (script_start_time - timedelta(days=1)).strftime("%Y_%m_%d")
ereyesterday = (script_start_time - timedelta(days=2)).strftime("%Y_%m_%d")
TODAY_WARDS_JSON = f"data/wards_{script_start_day}.json"
YESTERDAY_WARDS_JSON = f"data/wards_{yesterday}.json"
TODAY_STAKES_JSON = f"data/stakes_{script_start_day}.json"
YESTERDAY_STAKES_JSON = f"data/stakes_{yesterday}.json"
EREYESTERDAY_WARDS_JSON = f"data/wards_{ereyesterday}.json"
EREYESTERDAY_STAKES_JSON = f"data/stakes_{ereyesterday}.json"
setup_logging(
    json_logs=False, log_level="INFO", logfile=f"logs/units_{script_start_second}.log"
)

logger = structlog.get_logger()

# units are stakes, districts, wards, branches
# units by created date per month/year
# also total units by month/year. if we see lots of units "created" in recent years, while
# the total number of units is not increasing, then that's exactly the
# rearranging deck chairs on the titanic scenario that we imagine is happening

# generate chart of units created by year, by country, by state, etc.

# compare scraped data with the actual data here
# https://newsroom.churchofjesuschrist.org/facts-and-statistics


# setup daily scraper and store in filesystem for now in files like:
# wards.json
# stakes.json
# 2023_09_23_wards_added.json (diff of today's wards.json and yesterday's wards.json)
# 2023_09_23_stakes_added.json (diff of today's stakes.json and yesterday's stakes.json)
# 2023_09_23_wards_removed.json (diff of today's wards.json and yesterday's wards.json)
# after comparison, overwrite the wards.json and stakes.json files with the latest data

# also append to a daily summary file like:
# daily_summary.csv
# scrape_timestamp (date), total_stakes, total_districts, total_wards, total_branches, net_stakes, net_districts, net_wards, net_branches  # noqa: E501
# each day, overwrite the stakes.json and wards.json files with the latest data

# run via cron on chambersfam digitalocean machine

UNIT_TYPES = [
    # "WARD",
    "WARD__NEPALI",
    "WARD__STUDENT_MARRIED",
    "WARD__TONGAN_YSA",
    "WARD__JAPANESE",
    "WARD__RUSSIAN",
    "WARD__PORTUGUESE",
    "WARD__YSA",
    "WARD__TONGAN",
    "WARD__STUDENT",
    "WARD__MARSHALLESE",
    "WARD__PERSIAN",
    "WARD__SWAHILI",
    "WARD__VISITOR",
    "WARD__STUDENT_SINGLE",
    "WARD__FRENCH_YSA",
    "WARD__VIETNAMESE",
    "WARD__ASIAN_YSA",
    "WARD__ENGLISH",
    "WARD__CHINESE",
    "WARD__CHUUKIC_POHNPEIC",
    "WARD__KIRIBATI",
    "WARD__SAMOAN",
    "WARD__MAORI",
    "WARD__SPANISH",
    "WARD__FRENCH",
    "WARD__SINGLE_ADULT",
    "WARD__CAMBODIAN",
    "WARD__POHNPEIAN",
    "WARD__KAREN",
    "WARD__SPANISH_STUDENT_MARRIED",
    "WARD__TAGALOG",
    "WARD__DINKA_NUER",
    "WARD__NIUEAN",
    "WARD__MANDARIN",
    "WARD__KOREAN",
    "WARD__MILITARY",
    "WARD__SPANISH_YSA",
    "WARD__HMONG",
    "WARD__LAOTIAN",
    "WARD__CANTONESE",
    "WARD__SEASONAL",
    "WARD__NATIVE_AMERICAN",
    "WARD__HAITIAN_CREOLE",
    "WARD__FIJIAN",
    "WARD__TRANSITIONAL",
    "WARD__GERMAN",
    "WARD__DEAF",
]


class Address(BaseModel):
    """Data model for an address."""

    street1: str | None = None
    street2: str | None = None
    city: str | None = None
    county: str | None = None
    state: str | None = None
    stateId: int | None = None
    stateCode: str | None = None
    postalCode: str | None = None
    country: str
    countryId: int | None = None
    countryCode2: str | None = None
    countryCode3: str | None = None
    formatted: str | None = None
    lines: list[str] | None = None


class Language(BaseModel):
    """Data model for a language."""

    id: int
    code: str | None = None
    display: str | None = None


class Identifier(BaseModel):
    """Data model for an identifier of a ward."""

    facilityId: str | None = None
    structureId: str | None = None
    propertyId: int | None = None
    unitNumber: int
    orgId: int | None = None


class OrganizationType(BaseModel):
    """Data model for an organization type."""

    id: int
    code: str
    display: str


class Unit(BaseModel):
    """Data model for a Unit."""

    id: str
    type: str
    identifiers: Identifier
    name: str | None = None
    nameDisplay: str | None = None
    typeDisplay: str
    organizationType: OrganizationType | None = None
    address: Address | None = None
    phones: list[dict[str, str]] | None = None
    emails: list[dict[str, str]] | None = None
    contact: dict[str, Any] | None = None
    hours: dict[str, Any] | None = None
    timeZone: dict[str, str] | None = None
    language: Language | None = None
    provider: str
    specialized: bool | None = None
    notes: str | None = None
    created: str | None = None
    updated: str

    def __hash__(self):
        return hash((self.id, self.name))

    def __eq__(self, other: "Unit") -> bool:
        """Check if two wards are equivalent."""
        if self.id == other.id and self.name == other.name:
            return True
        return False


class Coordinate(BaseModel):
    """Data model for a coordinate."""

    lat: float
    lon: float
    city: str | None = None
    nearest: int | None = None


regions: list[dict[str, Any]] = [
    {
        "name": "North America",
        "min_lat": 24,
        "max_lat": 50,
        "min_lon": -126,
        "max_lon": -51,
        "nearest": 1000,
        "num_rows": 7,
        "num_columns": 20,
        # hardcode densely populated areas here to ensure coverage
        "coordinates": [
            # utah has ~5412 wards
            Coordinate(lon=-111.835, lat=41.735, city="Logan", nearest=1500),
            Coordinate(lon=-111.891, lat=40.875, city="Salt Lake City", nearest=1500),
            Coordinate(lon=-111.891, lat=40.875, city="Salt Lake City", nearest=1500),
            Coordinate(lon=-111.891, lat=40.775, city="Salt Lake City", nearest=1500),
            Coordinate(lon=-111.891, lat=40.675, city="Salt Lake City", nearest=1500),
            Coordinate(lon=-111.970, lat=40.658, city="Taylorsville", nearest=1500),
            Coordinate(lon=-112.044, lat=40.552, city="Daybreak", nearest=1500),
            Coordinate(lon=-111.910, lat=40.413, city="Lehi", nearest=1500),
            Coordinate(lon=-111.653, lat=40.374, city="Orem", nearest=1500),
            Coordinate(lon=-111.653, lat=40.274, city="Provo", nearest=1500),
            Coordinate(lon=-111.791, lat=40.038, city="Payson", nearest=1500),
            Coordinate(lon=-112.269, lat=38.586, city="Sevier", nearest=1500),
            Coordinate(lon=-113.143, lat=37.685, city="Cedar City", nearest=1500),
            Coordinate(lon=-113.646, lat=37.105, city="St George", nearest=1500),
            Coordinate(lon=-109.556, lat=40.454, city="Vernal", nearest=1500),
            Coordinate(lon=-109.572, lat=38.574, city="Moab", nearest=1500),
            Coordinate(lon=-114.099, lat=40.741, city="Wendover", nearest=1500),
            # idaho has ~1213 wards
            Coordinate(lon=-116.316, lat=43.600, city="Boise", nearest=1500),
            Coordinate(lon=-112.116, lat=43.493, city="Idaho Falls", nearest=1500),
            # california has ~1134 wards
            Coordinate(lon=-121.617, lat=38.562, city="Sacramento", nearest=1500),
            Coordinate(lon=-120.106, lat=36.785, city="Fresno", nearest=1500),
            Coordinate(lon=-119.036, lat=34.019, city="Los Angeles", nearest=1500),
            # texas has ~744 wards
            Coordinate(lon=-100.928, lat=23.110, city="Austin", nearest=1000),
            # arizona has ~930 wards
            Coordinate(lon=-111.873, lat=34.854, city="Sedona", nearest=1500),
            Coordinate(lon=-111.213, lat=32.155, city="Tucson", nearest=1500),
            # hawaii has ~143 wards
            Coordinate(lon=-157.881, lat=21.328, city="Honolulu", nearest=500),
            # washington has ~489 wards
            Coordinate(lon=-122.507, lat=47.613, city="Seattle", nearest=1000),
            # mexico has ~1863 wards
            Coordinate(lon=-116.749, lat=29.302, city="Sonora", nearest=2000),
            Coordinate(lon=-108.557, lat=28.042, city="Torreon", nearest=2000),
            Coordinate(lon=-103.491, lat=20.674, city="Guadalajara", nearest=2000),
            Coordinate(lon=-99.455, lat=19.391, city="Mexico City", nearest=2000),
            Coordinate(lon=-94.756, lat=16.245, city="Chiapas", nearest=2000),
            # TODO split up canada until i get ~493 wards
            Coordinate(lon=-151.942, lat=63.489, city="Yukon", nearest=500),
            Coordinate(lon=-113.556, lat=49.961, city="Alberta", nearest=500),
            Coordinate(lon=-105.443, lat=54.088, city="Manitoba", nearest=500),
            Coordinate(lon=-84.892, lat=53.535, city="Quebec", nearest=500),
        ],
    },
    {
        "name": "Europe",
        "min_lat": 36,
        "max_lat": 71,
        "min_lon": -35,
        "max_lon": 59,
        "nearest": 2000,
        "num_rows": 3,
        "num_columns": 5,
        "coordinates": [],  # hardcode densely populated areas here to ensure coverage
    },
    {
        "name": "Asia",
        "min_lat": 0,
        "max_lat": 50,
        "min_lon": 58,
        "max_lon": 145,
        "nearest": 2000,
        "num_rows": 3,
        "num_columns": 10,
        "coordinates": [],
    },
    {
        "name": "South America",
        "min_lat": -57,
        "max_lat": 24,
        "min_lon": -82,
        "max_lon": -34,
        "nearest": 2000,
        "num_rows": 10,
        "num_columns": 4,
        "coordinates": [
            # brazil has ~2176 wards
            # peru has ~779 wards
            # argentina has ~726 wards
        ],
    },
    {
        "name": "Africa",
        "min_lat": -35.888913,
        "max_lat": 37.207228,
        "min_lon": -26.001428,
        "max_lon": 63.916734,
        "nearest": 100,
        "num_rows": 5,
        "num_columns": 4,
        "coordinates": [],
    },
    {
        "name": "Australia",
        "min_lat": -43.634597,
        "max_lat": -10.059229,
        "min_lon": 113.338953,
        "max_lon": 153.569469,
        "nearest": 500,
        "num_rows": 2,
        "num_columns": 2,
        "coordinates": [],
    },
    {
        "name": "Antarctica",
        "min_lat": -90.0,
        "max_lat": -60.0,
        "min_lon": -180.0,
        "max_lon": 180.0,
        "nearest": 50,
        "num_rows": 3,
        "num_columns": 3,
        "coordinates": [],
    },
    {
        "name": "Atlantic Ocean",
        "min_lat": -40.0,
        "max_lat": 80.0,
        "min_lon": -100.0,
        "max_lon": -10.0,
        "nearest": 10,
        "num_rows": 8,
        "num_columns": 8,
        "coordinates": [],
    },
    {
        "name": "Pacific Ocean",
        "min_lat": -40.0,
        "max_lat": 80.0,
        "min_lon": 120.0,
        "max_lon": -120.0,
        "nearest": 20,
        "num_rows": 12,
        "num_columns": 12,
        "coordinates": [
            Coordinate(lon=178.423251, lat=-18.140505, city="Suva", nearest=1000)
        ],
    },
    {
        "name": "Indian Ocean",
        "min_lat": -40.0,
        "max_lat": 30.0,
        "min_lon": 40.0,
        "max_lon": 120.0,
        "nearest": 10,
        "num_rows": 8,
        "num_columns": 8,
        "coordinates": [],
    },
    {
        "name": "Arctic Ocean",
        "min_lat": 60.0,
        "max_lat": 90.0,
        "min_lon": -180.0,
        "max_lon": 180.0,
        "nearest": 10,
        "num_rows": 8,
        "num_columns": 8,
        "coordinates": [],
    },
    # Covered by Antarctica
    # {
    #     "name": "Southern Ocean",
    #     "min_lat": -90.0,
    #     "max_lat": -60.0,
    #     "min_lon": -180.0,
    #     "max_lon": 180.0,
    #     "nearest": 10,
    #     "num_rows": 8,
    #     "num_columns": 8,
    #     "coordinates": [],
    # },
]


class CountryStats(BaseModel):
    """Data model for country-based stats."""

    num_stakes: int
    num_districts: int
    num_wards: int
    num_branches: int
    stake_district_year_count: dict[str, int]
    ward_branch_year_count: dict[str, int]


@click.command()
@click.option(
    "--ward_web", default=False, is_flag=True, help="Get wards from web, vs local json."
)
@click.option(
    "--stake_web",
    default=False,
    is_flag=True,
    help="Get stakes from web, vs local json.",
)
@click.option("--skip_stats", default=False, is_flag=True, help="Skip stats.")
@click.option(
    "--show_figs",
    default=False,
    is_flag=True,
    help="Show charts of stakes/wards by year.",
)
def main(
    ward_web: bool,
    stake_web: bool,
    skip_stats: bool,
    show_figs: bool,
) -> None:
    """Main."""
    logger.info(
        "Starting script with flags.",
        ward_web=ward_web,
        stake_web=stake_web,
        skip_stats=skip_stats,
        show_figs=show_figs,
    )
    old_wards = get_yesterday_wards_json()
    ward_count_old, branch_count_old = count_unit_types(old_wards, "Ward")
    old_stakes = get_yesterday_stakes_json()
    stake_count_old, district_count_old = count_unit_types(old_stakes, "Stake")

    if stake_web:
        stakes = get_stakes_from_web()
    else:
        stakes = get_yesterday_stakes_json()

    if ward_web:
        wards = get_wards_from_web()
    else:
        wards = get_yesterday_wards_json()

    if not skip_stats:
        calculate_stake_stats(stakes, show_figs)
        calculate_ward_stats(wards, show_figs)

    # calculate_country_stats(stakes, wards)

    stake_count, district_count = count_unit_types(stakes, "Stake")
    ward_count, branch_count = count_unit_types(wards, "Ward")
    net_stakes = stake_count - stake_count_old
    net_districts = district_count - district_count_old
    net_wards = ward_count - ward_count_old
    net_branches = branch_count - branch_count_old

    append_daily_summary(
        stake_count,
        district_count,
        ward_count,
        branch_count,
        net_stakes,
        net_districts,
        net_wards,
        net_branches,
    )


def append_daily_summary(
    total_stakes: int,
    total_districts: int,
    total_wards: int,
    total_branches: int,
    net_stakes: int,
    net_districts: int,
    net_wards: int,
    net_branches: int,
) -> None:
    """Append daily summary."""
    # in case this runs multiple times in a day, check the last line of the file to see if it's
    # the same day as today. if so, then overwrite the last line with today's data
    # if not, then append today's data to the file
    dtype_dict = {
        "scrape_timestamp": str,  # Keep this column as a string
        "total_stakes": "Int64",
        "total_districts": "Int64",
        "total_wards": "Int64",
        "total_branches": "Int64",
        "net_stakes": "Int64",
        "net_districts": "Int64",
        "net_wards": "Int64",
        "net_branches": "Int64",
    }
    df = pd.read_csv("data/daily_summary.csv", dtype=dtype_dict)
    last_line = df.iloc[-1]
    last_line_date = last_line["scrape_timestamp"]
    if last_line_date == script_start_day:
        # overwrite last line
        df.iloc[-1] = [
            script_start_day,
            total_stakes,
            total_districts,
            total_wards,
            total_branches,
            net_stakes,
            net_districts,
            net_wards,
            net_branches,
        ]
    else:
        # append today's data
        df.loc[len(df)] = [
            script_start_day,
            total_stakes,
            total_districts,
            total_wards,
            total_branches,
            net_stakes,
            net_districts,
            net_wards,
            net_branches,
        ]

    df.to_csv("data/daily_summary.csv", index=False)


def count_unit_types(
    units: set[Unit], unit_type: Literal["Stake", "Ward"]
) -> tuple[int, int]:
    """Count the number of units of a given type.

    If `unit_type` is "Stake", then count the number of stakes and districts.
    If `unit_type` is "Ward", then count the number of wards and branches.

    Returns:
        tuple[int, int]: The number of units of the given type.
            (wards, branches) or (stakes, districts)
    """
    total_stakes: int = 0
    total_districts: int = 0
    total_wards: int = 0
    total_branches: int = 0
    unknown_units: set[Unit] = set()
    for unit in units:
        if not unit.organizationType:
            unknown_units.add(unit)
            continue
        if unit_type == "Stake":
            if unit.organizationType.display == "Stake":
                total_stakes += 1
            elif unit.organizationType.display == "District":
                total_districts += 1
        elif unit_type == "Ward":
            if unit.organizationType.display == "Ward":
                total_wards += 1
            elif unit.organizationType.display == "Branch":
                total_branches += 1

    if len(unknown_units) > 0:
        logger.warning(
            "Unknown organization types detected when trying to classify stakes and districts.",
            num_unknown_type=len(unknown_units),
            unknown_units=unknown_units,
            unit_type=unit_type,
        )

    if unit_type == "Stake":
        return total_stakes, total_districts
    elif unit_type == "Ward":
        return total_wards, total_branches
    else:
        raise ValueError(f"Invalid unit_type: {unit_type}")


def calculate_country_stats(stakes: set[Unit], wards: set[Unit]) -> None:
    """Calculate country-level stats."""
    pass


def calculate_stake_stats(stakes: set[Unit], show_figs: bool = False) -> None:
    """Calculate stake stats."""
    count_of_stakes_by_country: dict[str, int] = defaultdict(int)
    for stake in stakes:
        if not stake.address:
            count_of_stakes_by_country["Unknown"] += 1
        elif not stake.address.country:
            count_of_stakes_by_country["Unknown"] += 1
        else:
            country = stake.address.country
            count_of_stakes_by_country[country] += 1
    logger.info(
        "Stakes by country.",
        count_of_stakes_by_country=dict(count_of_stakes_by_country),
    )
    # write to file
    with open("stakes_by_country.json", "w") as f:
        json.dump(count_of_stakes_by_country, f, indent=4)

    count_of_stakes_districts_created_by_year: dict[str, dict[str, int]] = defaultdict(
        lambda: defaultdict(int)
    )
    for stake in stakes:
        if not stake.created:
            count_of_stakes_districts_created_by_year["Unknown"]["Unknown"] += 1
        else:
            year = stake.created.split("-")[0]
            if not stake.organizationType:
                count_of_stakes_districts_created_by_year[year]["Unknown"] += 1
            else:
                org_type = stake.organizationType.display
                # org_type is either `Stake` or `District`
                count_of_stakes_districts_created_by_year[year][org_type] += 1
    # sort by year
    count_of_stakes_districts_created_by_year["1830"] = {"Stake": 0, "District": 0}
    count_of_stakes_districts_created_by_year = dict(
        sorted(
            count_of_stakes_districts_created_by_year.items(), key=lambda item: item[0]
        )
    )
    logger.info(
        "Stakes and districts created by year.",
        count_of_stakes_districts_created_by_year=json.loads(
            json.dumps(count_of_stakes_districts_created_by_year)
        ),
    )

    if not show_figs:
        return

    # create a bar chart of stakes and districts created by year
    # Extract years and counts
    years = list(count_of_stakes_districts_created_by_year.keys())
    stake_counts = []
    district_counts = []
    for year in years:
        stake_counts.append(count_of_stakes_districts_created_by_year[year]["Stake"])
        district_counts.append(
            count_of_stakes_districts_created_by_year[year]["District"]
        )

    # Create a bar chart
    plt.figure(figsize=(10, 6))
    plt.bar(years, stake_counts, color="skyblue", label="Stakes")
    plt.bar(
        years,
        district_counts,
        bottom=stake_counts,
        color="royalblue",
        label="Districts",
    )
    plt.xlabel("Year")
    plt.ylabel("Count of Stakes and Districts Created")
    plt.title("Stakes and Districts Created by Year")
    plt.xticks(rotation=45)  # Rotate x-axis labels for better readability

    plt.legend()
    # Show only every 5th year on the x-axis
    plt.xticks(years[::5])

    # Show the chart
    plt.tight_layout()
    plt.show()


def calculate_ward_stats(wards: set[Unit], show_figs: bool = False) -> None:
    """Calculate ward stats."""
    all_states: set[str] = set()
    for unit in wards:
        if not unit.address:
            all_states.add("Unknown")
        elif not unit.address.state:
            all_states.add("Unknown")
        else:
            if not unit.address.country or unit.address.country != "United States":
                continue
            all_states.add(unit.address.state)
    count_of_wards_by_state: dict[str, dict[str, int]] = {}
    # initialize all state values to 0
    for state in all_states:
        count_of_wards_by_state[state] = {
            "Ward": 0,
            "Branch": 0,
            "Total": 0,
            "Unknown": 0,
        }
    for unit in wards:
        if not unit.address:
            count_of_wards_by_state["Unknown"]["Unknown"] += 1
        elif not unit.address.state:
            count_of_wards_by_state["Unknown"]["Unknown"] += 1
        else:
            if not unit.address.country or unit.address.country != "United States":
                continue
            state = unit.address.state
            if not unit.organizationType:
                count_of_wards_by_state[state]["Unknown"] += 1
            else:
                org_type = unit.organizationType.display
                # org_type is either `Ward` or `Branch`
                count_of_wards_by_state[state][org_type] += 1

    # add total for each state
    for state in count_of_wards_by_state:
        count_of_wards_by_state[state]["Total"] = sum(
            count_of_wards_by_state[state].values()
        )

    # sort by state name
    count_of_wards_by_state = dict(sorted(count_of_wards_by_state.items()))
    logger.info(
        "Wards by state.",
        count_of_wards_by_state=json.loads(json.dumps(count_of_wards_by_state)),
    )
    # obtain list of all countries
    countries: list[str] = []
    for unit in wards:
        if not unit.address:
            countries.append("Unknown")
        elif not unit.address.country:
            countries.append("Unknown")
        else:
            countries.append(unit.address.country)

    count_of_units_by_country: dict[str, dict[str, int]] = {}
    # initialize all country values to 0
    for country in set(countries):
        count_of_units_by_country[country] = {
            "Ward": 0,
            "Branch": 0,
            "Total": 0,
            "Unknown": 0,
        }
    count_of_units_by_country["Unknown"] = {
        "Ward": 0,
        "Branch": 0,
        "Total": 0,
        "Unknown": 0,
    }
    for unit in wards:
        if not unit.address:
            if not unit.organizationType:
                count_of_units_by_country["Unknown"]["Unknown"] += 1
            else:
                org_type = unit.organizationType.display
                # org_type is either `Ward` or `Branch`
                count_of_units_by_country["Unknown"][org_type] += 1
        elif not unit.address.country:
            if not unit.organizationType:
                count_of_units_by_country["Unknown"]["Unknown"] += 1
            else:
                org_type = unit.organizationType.display
                # org_type is either `Ward` or `Branch`
                count_of_units_by_country["Unknown"][org_type] += 1
        else:
            country = unit.address.country
            if not unit.organizationType:
                count_of_units_by_country[country]["Unknown"] += 1
            else:
                org_type = unit.organizationType.display
                # org_type is either `Ward` or `Branch`
                count_of_units_by_country[country][org_type] += 1

    # add total for each country
    for country in count_of_units_by_country:
        count_of_units_by_country[country]["Total"] = sum(
            count_of_units_by_country[country].values()
        )
    # sort dict keys
    count_of_units_by_country = dict(sorted(count_of_units_by_country.items()))
    logger.info(
        "Units by country.",
        count_of_units_by_country=count_of_units_by_country,
    )

    count_of_wards_branches_created_by_year: dict[str, dict[str, int]] = defaultdict(
        lambda: defaultdict(int)
    )

    for unit in wards:
        if not unit.created:
            count_of_wards_branches_created_by_year["Unknown"]["Unknown"] += 1
        else:
            year = unit.created.split("-")[0]
            if not unit.organizationType:
                count_of_wards_branches_created_by_year[year]["Unknown"] += 1
            else:
                org_type = unit.organizationType.display
                # org_type is either `Ward` or `Branch`
                count_of_wards_branches_created_by_year[year][org_type] += 1

    # sort by year
    count_of_wards_branches_created_by_year["1830"] = {"Ward": 0, "Branch": 0}
    count_of_wards_branches_created_by_year = dict(
        sorted(
            count_of_wards_branches_created_by_year.items(), key=lambda item: item[0]
        )
    )
    logger.info(
        "Wards created by year.",
        count_of_wards_created_by_year=json.loads(
            json.dumps(count_of_wards_branches_created_by_year)
        ),
    )

    if not show_figs:
        return
    # create a bar chart of wards and branches created by year
    # Extract years and ward_counts, branch_counts
    years = list(count_of_wards_branches_created_by_year.keys())
    ward_counts = []
    branch_counts = []
    for year in years:
        ward_counts.append(count_of_wards_branches_created_by_year[year]["Ward"])
        branch_counts.append(count_of_wards_branches_created_by_year[year]["Branch"])

    # Create a bar chart
    plt.figure(figsize=(10, 6))
    plt.bar(years, ward_counts, color="skyblue", label="Wards")
    plt.bar(
        years, branch_counts, bottom=ward_counts, color="royalblue", label="Branches"
    )
    plt.xlabel("Year")
    plt.ylabel("Count of Wards Created")
    plt.title("Wards Created by Year")
    plt.xticks(rotation=45)  # Rotate x-axis labels for better readability

    plt.legend()
    # Show only every 5th year on the x-axis
    plt.xticks(years[::5])

    # Show the chart
    plt.tight_layout()
    plt.show()


def get_stakes_json() -> set[Unit]:
    """Get stakes from today's json file."""
    stakes: set[Unit] = set()
    with open(TODAY_STAKES_JSON, "r") as f:
        stakes_json = json.load(f)
    for stake_json in stakes_json["stakes"]:
        stake = Unit.model_validate(stake_json)
        stakes.add(stake)
    return stakes


def get_yesterday_stakes_json() -> set[Unit]:
    """Get stakes from yesterday's json file."""
    stakes: set[Unit] = set()
    with open(YESTERDAY_STAKES_JSON, "r") as f:
        stakes_json = json.load(f)
    for stake_json in stakes_json["stakes"]:
        stake = Unit.model_validate(stake_json)
        stakes.add(stake)
    return stakes


def get_wards_json() -> set[Unit]:
    """Get wards from today's json file."""
    wards: set[Unit] = set()
    with open(TODAY_WARDS_JSON, "r") as f:
        wards_json = json.load(f)
    for ward_json in wards_json["wards"]:
        ward = Unit.model_validate(ward_json)
        wards.add(ward)
    return wards


def get_yesterday_wards_json() -> set[Unit]:
    """Get wards from json."""
    wards: set[Unit] = set()
    with open(YESTERDAY_WARDS_JSON, "r") as f:
        wards_json = json.load(f)
    for ward_json in wards_json["wards"]:
        ward = Unit.model_validate(ward_json)
        wards.add(ward)
    return wards


def get_stakes_from_web() -> set[Unit]:
    """Get stakes from web."""
    headers = {
        "Accept": "application/json",
        "Referer": "https://maps.churchofjesuschrist.org/",
    }
    base_url = (
        "https://maps.churchofjesuschrist.org/api/maps-proxy/v2/locations/identify"
    )
    layers = "STAKE"
    filters = ""
    coordinates = [
        Coordinate(lon=-111.891, lat=40.875, city="Salt Lake City", nearest=1000),
        Coordinate(lon=-119.036, lat=34.019, city="Los Angeles", nearest=1000),
        Coordinate(lon=-99.455, lat=19.391, city="Mexico City", nearest=1000),
        Coordinate(lon=-48.020, lat=-15.722, city="Brasilia", nearest=1000),
        Coordinate(lon=-77.097, lat=38.894, city="Washington DC", nearest=1000),
        Coordinate(lon=13.260, lat=52.507, city="Berlin", nearest=1000),
        Coordinate(lon=151.209, lat=33.869, city="Sydney", nearest=1000),
    ]
    scrape_start_time = time.time()
    stakes: set[Unit] = set()
    for coordinate in coordinates:
        region_start_time = time.time()
        nearest = coordinate.nearest
        coord = f"{coordinate.lon},{coordinate.lat}"
        url = f"{base_url}?layers={layers}&filters={filters}&coordinates={coord}&nearest={nearest}"
        response = requests.get(url, headers=headers)
        data = response.json()
        stake_models = [Unit.model_validate(stake) for stake in data]

        pre_update_count = len(stakes)
        stakes.update(stake_models)
        post_update_count = len(stakes)
        region_end_time = time.time()
        logger.info(
            "Finished Stakes for city.",
            coordinates=coord,
            region_name=coordinate.city,
            region_time=region_end_time - region_start_time,
            num_stakes_added=post_update_count - pre_update_count,
            num_duplicates=len(data) - (post_update_count - pre_update_count),
            max_stakes=nearest,
            num_api_requests=1,
        )
    scrape_end_time = time.time()
    logger.info(
        "Finished scraping.",
        total_stakes=len(stakes),
        total_api_requests=len(coordinates),
        total_elapsed_time=scrape_end_time - scrape_start_time,
    )

    # compare with old stakes.json
    old_stakes = get_yesterday_stakes_json()
    write_daily_files(old_units=old_stakes, new_units=stakes, unit_type="Stake")

    write_units_json(units=stakes, unit_type="Stake")
    return stakes


def write_daily_files(
    old_units: set[Unit], new_units: set[Unit], unit_type: Literal["Stake", "Ward"]
) -> None:
    units_added: set[Unit] = set()
    small_units_added: set[Unit] = set()
    units_removed: set[Unit] = set()
    small_units_removed: set[Unit] = set()
    for unit in new_units:
        if unit not in old_units:
            if unit_type == "Stake":
                if unit.organizationType and unit.organizationType.display == "Stake":
                    units_added.add(unit)
                elif (
                    unit.organizationType
                    and unit.organizationType.display == "District"
                ):
                    small_units_added.add(unit)
                else:
                    logger.warning(
                        "Unknown organization type detected when trying to classify stakes and districts.",
                        stake=unit,
                    )
            elif unit_type == "Ward":
                if unit.organizationType and unit.organizationType.display == "Ward":
                    units_added.add(unit)
                elif (
                    unit.organizationType and unit.organizationType.display == "Branch"
                ):
                    small_units_added.add(unit)
                else:
                    logger.warning(
                        "Unknown organization type detected when trying to classify wards and branches.",
                        ward=unit,
                    )
            else:
                raise ValueError(f"Invalid unit_type: {unit_type}")

    for unit in old_units:
        if unit not in new_units:
            if unit_type == "Stake":
                if unit.organizationType and unit.organizationType.display == "Stake":
                    units_removed.add(unit)
                elif (
                    unit.organizationType
                    and unit.organizationType.display == "District"
                ):
                    small_units_removed.add(unit)
                else:
                    logger.warning(
                        "Unknown organization type detected when trying to classify stakes and districts.",
                        stake=unit,
                    )
            elif unit_type == "Ward":
                if unit.organizationType and unit.organizationType.display == "Ward":
                    units_removed.add(unit)
                elif (
                    unit.organizationType and unit.organizationType.display == "Branch"
                ):
                    small_units_removed.add(unit)
                else:
                    logger.warning(
                        "Unknown organization type detected when trying to classify wards and branches.",
                        ward=unit,
                    )
            else:
                raise ValueError(f"Invalid unit_type: {unit_type}")

    logger.info(
        f"Net units today.",
        unit_type=unit_type,
        units_added=len(units_added),
        units_removed=len(units_removed),
        small_units_added=len(small_units_added),
        small_units_removed=len(small_units_removed),
    )
    # write to 4 files
    daily_dir = Path(f"data/daily/{script_start_day}")
    daily_dir.mkdir(parents=True, exist_ok=True)
    if unit_type == "Stake":
        file_1 = Path(daily_dir / "stakes_added.json")
        file_2 = Path(daily_dir / "stakes_removed.json")
        file_3 = Path(daily_dir / "districts_added.json")
        file_4 = Path(daily_dir / "districts_removed.json")
    elif unit_type == "Ward":
        file_1 = Path(daily_dir / "wards_added.json")
        file_2 = Path(daily_dir / "wards_removed.json")
        file_3 = Path(daily_dir / "branches_added.json")
        file_4 = Path(daily_dir / "branches_removed.json")

    with open(file_1, "w") as f:
        json.dump([unit.model_dump() for unit in units_added], f, indent=4)

    with open(file_2, "w") as f:
        json.dump([unit.model_dump() for unit in units_removed], f, indent=4)

    with open(file_3, "w") as f:
        json.dump([unit.model_dump() for unit in small_units_added], f, indent=4)

    with open(file_4, "w") as f:
        json.dump([unit.model_dump() for unit in small_units_removed], f, indent=4)
    logger.info("Finished writing to daily files.", unit_type=unit_type)


def get_wards_from_web() -> set[Unit]:
    """Get wards from web."""

    headers = {
        "Accept": "application/json",
        "Referer": "https://maps.churchofjesuschrist.org/",
    }

    base_url = (
        "https://maps.churchofjesuschrist.org/api/maps-proxy/v2/locations/identify"
    )

    # Calculate step sizes for latitude and longitude
    for region in regions:
        num_rows = region["num_rows"]
        num_columns = region["num_columns"]
        # split region into 20 sets of coordinates
        lat_step = (region["max_lat"] - region["min_lat"]) / num_rows
        lon_step = (region["max_lon"] - region["min_lon"]) / num_columns
        for i in range(num_rows):
            for j in range(num_columns):
                # Calculate the coordinates for the centroid of the current cell
                centroid_lat = region["min_lat"] + (i + 0.5) * lat_step
                centroid_lon = region["min_lon"] + (j + 0.5) * lon_step
                region["coordinates"].append(
                    Coordinate(lon=centroid_lon, lat=centroid_lat)
                )

    logger.info(
        "Starting scrape of wards from web.",
        num_regions=len(regions),
    )
    wards: set[Unit] = set()
    scrape_start_time = time.time()
    lock = threading.Lock()
    total_requests = 0
    filters = ""
    # get wards for all the different unit_types (except WARD, we'll do that last)
    # heuristically, the largest unit_type is WARD__YSA with ~1000 wards
    for unit_type in UNIT_TYPES:
        coord = Coordinate(lon=0, lat=0, city="Global", nearest=2000)
        _get_wards_at_coords(
            wards=wards,
            coord=coord,
            base_url=base_url,
            layers=unit_type,
            filters=filters,
            headers=headers,
            region_name="Global",
            nearest=2000,
            lock=lock,
        )
        total_requests += 1

    # WARD is the largest unit_type with ~27000.
    layers = "WARD"
    for region in regions:
        region_start_time = time.time()
        pre_region_update_count = len(wards)
        with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
            executor.map(
                lambda coord: _get_wards_at_coords(
                    wards=wards,
                    coord=coord,
                    base_url=base_url,
                    layers=layers,
                    filters=filters,
                    headers=headers,
                    nearest=region["nearest"],
                    region_name=region["name"],
                    lock=lock,
                ),
                region["coordinates"],
            )
        region_requests = len(region["coordinates"])
        total_requests += region_requests
        post_region_update_count = len(wards)
        region_end_time = time.time()
        logger.info(
            "Finished region.",
            region_name=region["name"],
            region_time=region_end_time - region_start_time,
            num_wards_added=post_region_update_count - pre_region_update_count,
            num_api_requests=region_requests,
        )

    scrape_end_time = time.time()
    logger.info(
        "Finished scraping.",
        total_elapsed_time=scrape_end_time - scrape_start_time,
        total_wards=len(wards),
        total_api_requests=total_requests,
    )
    # compare with old wards.json
    old_wards = get_yesterday_wards_json()
    write_daily_files(old_units=old_wards, new_units=wards, unit_type="Ward")

    write_units_json(wards, unit_type="Ward")
    return wards


def write_units_json(units: set[Unit], unit_type: Literal["Stake", "Ward"]) -> None:
    """Write wards/stakes to json.

    This method removes the old json files if they exist.
    e.g. If today is 2023_10_07, then we will delete 2023_10_05 and leave 2023_10_06.
    """
    units_dict = [unit.model_dump() for unit in units]
    if unit_type == "Stake":
        file_path = Path(TODAY_STAKES_JSON)
        ereyesterday_path = Path(EREYESTERDAY_STAKES_JSON)
    elif unit_type == "Ward":
        file_path = Path(TODAY_WARDS_JSON)
        ereyesterday_path = Path(EREYESTERDAY_WARDS_JSON)
    else:
        raise ValueError(f"Invalid unit_type: {unit_type}")
    # delete ereyesterday's file
    if ereyesterday_path.exists():
        ereyesterday_path.unlink()

    # write today's file
    file_path.parent.mkdir(parents=True, exist_ok=True)
    with open(file_path, "w") as f:
        if unit_type == "Stake":
            key = "stakes"
        elif unit_type == "Ward":
            key = "wards"
        else:
            raise ValueError(f"Invalid unit_type: {unit_type}")
        units_output = {
            key: units_dict,
            "timestamp": script_start_time.isoformat(),
        }
        json.dump(units_output, f, indent=4)


def _get_wards_at_coords(
    wards: set[Unit],
    coord: Coordinate,
    base_url: str,
    layers: str,
    filters: str,
    headers: dict[str, str],
    nearest: int,
    region_name: str,
    lock: threading.Lock,
) -> None:
    """Get wards at coordinates."""
    pre_update_count = 0
    post_update_count = 0
    coordinates = f"{coord.lon},{coord.lat}"
    k = coord.nearest or nearest
    url = f"{base_url}?layers={layers}&filters={filters}&coordinates={coordinates}&nearest={k}"  # noqa: E501
    start_time = time.time()

    try:
        response = requests.get(url, headers=headers)
    except requests.exceptions.ConnectionError as e:
        logger.error("Error getting wards from web. Retrying...", error=e)
        try:
            response = requests.get(url, headers=headers)
        except Exception as e:
            logger.error(
                "Error getting wards from web on retry. Skipping.",
                error=e,
                region_name=region_name,
                coord=coord,
            )
            return

    end_time = time.time()
    if response.status_code != 200:
        logger.error("Error getting wards from web", status_code=response.status_code)

    data = response.json()
    ward_models: list[Unit] = []
    for ward_json in data:
        ward_model = Unit.model_validate(ward_json)
        ward_models.append(ward_model)

    with lock:
        pre_update_count = len(wards)
        wards.update(ward_models)
        post_update_count = len(wards)

    logger.info(
        "Wards added.",
        region_name=region_name,
        unit_type=layers,
        coordinates=coordinates,
        num_wards_added=post_update_count - pre_update_count,
        num_duplicates=len(data) - (post_update_count - pre_update_count),
        max_wards=k,
        city=coord.city,
        elapsed_time=end_time - start_time,
    )


if __name__ == "__main__":
    main()
