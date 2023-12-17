"""Analyze units data."""

# Choose start/end date and calculate:
# - net stakes/districts/wards/branches per country
# - net stakes/districts/wards/branches per state

# WARN: churn may not be interesting because the locator weekly drops 300 wards and 100 branches
# and then readds them
# churn per country/state where churn is:
#   - units opened/closed

from datetime import datetime, timedelta
import json
import pathlib
from typing import Any
from lds_datasets.units.schemas import DateRange, UnitType
import structlog

logger = structlog.get_logger(__name__)

unit_type_to_file_added_mapping = {
    "stake": "stakes_added.json",
    "district": "districts_added.json",
    "ward": "wards_added.json",
    "branch": "branches_added.json",
}
unit_type_to_file_removed_mapping = {
    "stake": "stakes_removed.json",
    "district": "districts_removed.json",
    "ward": "wards_removed.json",
    "branch": "branches_removed.json",
}


def get_daily_folder_names(date_range: DateRange) -> list[str]:
    """Get a list of daily folder names, given the date range.

    Folders are named like: 2023_10_01, 2023_10_02, etc.
    """
    total_days = (date_range.end_date - date_range.start_date).days
    if total_days > 0:
        return [
            (date_range.start_date + timedelta(days=i)).strftime("%Y_%m_%d")
            for i in range(total_days)
        ]
    else:
        return []


def units_per_country(
    unit_type: UnitType, date_range: DateRange
) -> dict[str, dict[str, Any]]:
    """Count of unit_type changes, by country."""
    add_file_name = unit_type_to_file_added_mapping[unit_type.value]
    remove_file_name = unit_type_to_file_removed_mapping[unit_type.value]

    daily_folder_names = get_daily_folder_names(date_range)
    country_counts: dict[str, dict[str, Any]] = {}
    # e.g.
    # {
    #     "USA": {"added": 7, "removed": 5},
    #     "Canada": {"added": 0, "removed": 0},
    #     "Mexico": {"added": 0, "removed": 0},
    # }
    for dfn in daily_folder_names:
        daily_path = f"data/daily/{dfn}"
        add_path = f"{daily_path}/{add_file_name}"
        remove_path = f"{daily_path}/{remove_file_name}"

        if not pathlib.Path(daily_path).exists():
            logger.warning(f"Could not open {add_path} or {remove_path}")
            continue
        with open(add_path) as add_f, open(remove_path) as remove_f:
            add_data = json.load(add_f)
            remove_data = json.load(remove_f)
            for unit in add_data:
                if "address" not in unit or unit["address"] is None:
                    logger.warning("No address in unit", unit=unit)
                    continue
                country = unit["address"]["country"]
                if country not in country_counts:
                    country_counts[country] = {"added": 0, "removed": 0}
                    country_counts[country]["added_unit_names"] = []
                    country_counts[country]["removed_unit_names"] = []
                country_counts[country]["added"] += 1
                country_counts[country]["added_unit_names"].append(unit["name"])
            for unit in remove_data:
                if "address" not in unit or unit["address"] is None:
                    logger.warning("No address in unit", unit=unit)
                    continue
                country = unit["address"]["country"]
                if country not in country_counts:
                    country_counts[country] = {"added": 0, "removed": 0}
                    country_counts[country]["added_unit_names"] = []
                    country_counts[country]["removed_unit_names"] = []
                country_counts[country]["removed"] += 1
                country_counts[country]["removed_unit_names"].append(unit["name"])

    # sort all the unit_names lists
    for country, counts in country_counts.items():
        counts["added_unit_names"].sort()
        counts["removed_unit_names"].sort()
    return country_counts


oct_to_dec = DateRange(start_date=datetime(2023, 10, 1), end_date=datetime(2023, 12, 1))
counts_per_unit_type = {}
for unit_type in UnitType:
    print(unit_type)
    counts_per_unit_type[unit_type.value] = units_per_country(unit_type, oct_to_dec)

print(json.dumps(counts_per_unit_type, indent=2))


def transform_country_centric(
    original_data: dict[str, dict[str, Any]]
) -> dict[str, dict[str, Any]]:
    """Transform the data from a unit_type-centric view to a country-centric view."""
    transformed_data: dict[str, dict[str, Any]] = {}
    for unit_type, countries in original_data.items():
        for country, values in countries.items():
            if country not in transformed_data:
                transformed_data[country] = {}
            transformed_data[country][unit_type] = values
    # add "net" values for each unit_type
    for country, values in transformed_data.items():
        for unit_type in values:
            values[unit_type]["net"] = (
                values[unit_type]["added"] - values[unit_type]["removed"]
            )
    return transformed_data


# Transform the data
counts_per_country = transform_country_centric(counts_per_unit_type)
# print(json.dumps(counts_per_country, indent=2))


# transform to csv and only include countries with net changes
def to_csv(data: dict[str, dict[str, Any]]) -> str:
    """Transform the data to csv."""
    csv = "country,stake,district,ward,branch\n"
    for country, values in data.items():
        stake_val = values.get("stake", {}).get("net", 0)
        district_val = values.get("district", {}).get("net", 0)
        ward_val = values.get("ward", {}).get("net", 0)
        branch_val = values.get("branch", {}).get("net", 0)
        csv += f"{country},{stake_val},{district_val},{ward_val},{branch_val}\n"
    return csv


print(to_csv(counts_per_country))
