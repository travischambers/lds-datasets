"""Meetinghouse scraper."""

# Scrape all meetinghouses from https://maps.churchofjesuschrist.org/
# and save them to a JSON file.

import json
from typing import Any

import matplotlib.pyplot as plt
import requests
import structlog

logger = structlog.get_logger()


def main():
    """Main function."""
    # calculate_stats_via_polars()
    # calculate_stats_via_pandas()
    # scrape all the meetinghouses
    buildings = get_buildings_from_json()
    # # buildings = get_buildings_from_web()
    logger.info(f"Total buildings found: {len(buildings)}")

    calculate_stats(buildings)


def calculate_stats_via_pandas():
    """Use pandas to get some summary stats."""
    import pandas as pd

    logger.info("Reading buildings from buildings.json...")
    df = pd.read_json("buildings.json")
    logger.info("Calculating stats via pandas...")
    summary_stats = df.describe(include="all", percentiles=None)
    logger.info(f"Summary stats:\n {summary_stats}")


def calculate_stats_via_polars():
    """Use polars to get some summary stats."""
    import polars as pl

    logger.info("Reading buildings from buildings.json...")
    df = pl.read_json("buildings.json")
    logger.info("Calculating stats via polars...")
    summary_stats = df.describe(percentiles=None)
    logger.info(f"Summary stats:\n {summary_stats}")


def avg_size_by_country(buildings: dict[str, Any]):
    """Calculate building size per country."""
    no_interior_size = []
    no_property_size = []
    no_address = []
    wrong_internal_size = []
    zero_size = []
    global_total_size = 0
    countries = {}
    for building in buildings:
        skip = False
        if (
            "address" not in building
            or "country" not in building["address"]
            or building["address"]["formatted"] == "No Address Data"
        ):
            skip = True
            no_address.append(building)
        if "interiorSize" not in building or "value" not in building["interiorSize"]:
            skip = True
            no_interior_size.append(building)
        if "propertySize" not in building or "value" not in building["propertySize"]:
            skip = True
            no_property_size.append(building)

        if skip:
            continue

        # verified prior that all interiorSize types are "SQUARE_METER"
        bldg_country = building["address"]["country"]
        bldg_interior_size = building["interiorSize"]["value"]
        bldg_property_size = building["propertySize"]["value"]

        if bldg_interior_size > bldg_property_size:
            wrong_internal_size.append(building)
            skip = True

        if bldg_interior_size == 0:
            zero_size.append(building)
            skip = True

        if skip:
            continue

        # calculate global average size
        global_total_size += bldg_interior_size
        if bldg_country not in countries:
            # initialize country
            countries[bldg_country] = {}
            countries[bldg_country]["total_size_sq_m"] = 0
            countries[bldg_country]["count"] = 0

        countries[bldg_country]["total_size_sq_m"] += bldg_interior_size
        countries[bldg_country]["count"] += 1

    # calculate avg size for each country
    for country in countries:
        total_size = countries[country]["total_size_sq_m"]
        total_count = countries[country]["count"]
        countries[country]["avg_size_sq_m"] = int(total_size / total_count)
        countries[country]["avg_size_sq_ft"] = int((total_size / total_count) * 10.7639)

    # sort countries by average size
    countries = dict(
        sorted(
            countries.items(),
            key=lambda item: item[1]["avg_size_sq_m"],
            reverse=True,
        )
    )
    num_buildings_with_size = len(buildings) - len(no_interior_size) - len(no_address)
    global_avg_size = f"{int(global_total_size / num_buildings_with_size)}"
    logger.info(f"Avg size by country: \n{json.dumps(countries, indent=4)}")
    logger.info(f"Total buildings with no address: {len(no_address)}")
    logger.info(f"Total buildings with no interior size: {len(no_interior_size)}")
    logger.info(f"Total wrong internal size: {len(wrong_internal_size)}")
    logger.info(f"Total zero size: {len(zero_size)}")
    logger.info(
        f"Global average size_sq_meters: {int(global_avg_size)}, sq_ft: {int(int(global_avg_size) * 10.7639)}"
    )


def get_units_at_time(buildings: dict[str, Any], city: str, time: str):
    """Given a city and time, find all units meeting at that time.

    time format: "Su 11:00"
    """
    matches = []
    for building in buildings:
        if "address" not in building or "city" not in building["address"]:
            continue
        if building["address"]["city"].upper() != city.upper():
            continue

        for associated in building["associated"]:
            if "subType" in associated and associated["subType"] == "YSA":
                continue
            if "hours" not in associated:
                continue
            if time not in associated["hours"]["code"]:
                continue

            matches.append(building)

    logger.info(f"{json.dumps(matches, indent=4)}")
    logger.info(
        f"Found {len(matches)} buildings with units meeting at {time} in {city}"
    )
    return matches


def get_unit_types(buildings: dict[str, Any]):
    """Get unit types."""
    countries = {}

    associated = []
    types: set[str] = set()
    subtypes: set[str] = set()
    count_of_building_with_no_units = 0
    buildings_with_no_units = []
    buildings_with_no_address = []
    buildings_with_no_address_but_units = []
    for building in buildings:
        has_address = True
        if "address" not in building:
            buildings_with_no_address.append(building)
            has_address = False
        elif building["address"]["formatted"] == "No Address Data":
            buildings_with_no_address.append(building)
            has_address = False
        else:
            if building["address"]["country"] not in countries:
                countries[building["address"]["country"]] = {}
                countries[building["address"]["country"]]["units"] = {}
                countries[building["address"]["country"]]["units"]["count"] = 0
                countries[building["address"]["country"]]["units"][
                    "building_with_no_units"
                ] = 0
                countries[building["address"]["country"]]["buildings"] = 1
            else:
                countries[building["address"]["country"]]["buildings"] += 1

        if "associated" not in building:
            count_of_building_with_no_units += 1
            buildings_with_no_units.append(building)
            if has_address:
                countries[building["address"]["country"]]["units"][
                    "building_with_no_units"
                ] += 1
        else:
            if not has_address:
                buildings_with_no_address_but_units.append(building)
            else:
                countries[building["address"]["country"]]["units"]["count"] += len(
                    building["associated"]
                )
            for assoc in building["associated"]:
                associated.append(assoc)
                types.add(assoc["type"])
                if "subType" in assoc:
                    subtypes.add(assoc["subType"])

    logger.info(f"Total units: {len(associated)}")
    logger.info(f"Total unit types: {len(types)}")
    logger.info(f"Total buildings with no units: {count_of_building_with_no_units}")
    logger.info(f"Total country count: {len(countries.keys())}")
    logger.info(f"Total buildings with no address: {len(buildings_with_no_address)}")
    logger.info(
        f"Total buildings with no address, but has units: {len(buildings_with_no_address_but_units)}"
    )
    # print number of associated by type
    type_counts = {}
    for t in types:
        count = len([a for a in associated if a["type"] == t])
        type_counts[t] = count
    subtype_counts = {}
    associated_with_subtype = [a for a in associated if "subType" in a]
    associated_with_no_subtype = [a for a in associated if "subType" not in a]
    subtype_counts["NULL"] = len(associated_with_no_subtype)
    for st in subtypes:
        count = len([a for a in associated_with_subtype if a["subType"] == st])
        subtype_counts[st] = count

    logger.info(f"Unit by type: {json.dumps(type_counts, indent=2)}")
    logger.info(f"Unit by subtype: {json.dumps(subtype_counts, indent=2)}")


def get_buildings_by_country(buildings: dict[str, Any]):
    """Get buildings by country."""
    buildings_by_country = {}
    for building in buildings:
        if "address" not in building:
            continue
        if building["address"]["country"] not in buildings_by_country:
            buildings_by_country[building["address"]["country"]] = []
        buildings_by_country[building["address"]["country"]].append(building)

    # print buildings and units by country
    logger.info(f"Buildings by country: {json.dumps(buildings_by_country, indent=2)}")

    return buildings_by_country


def get_num_units_per_building(buildings: dict[str, Any]):
    """Get number of units per building."""
    num_buildings_with_units = {}
    for building in buildings:
        if "associated" not in building:
            if 0 not in num_buildings_with_units:
                num_buildings_with_units[0] = 0
            num_buildings_with_units[0] += 1
        else:
            num_units = len(building["associated"])
            if num_units not in num_buildings_with_units:
                num_buildings_with_units[num_units] = 0
            num_buildings_with_units[num_units] += 1

    logger.info(
        f"Count of buildings with number of units: {json.dumps(num_buildings_with_units, indent=2)}"
    )

    # print number of units per building
    summarized = {}
    for k, v in num_buildings_with_units.items():
        if k >= 5:
            if "5+" not in summarized:
                summarized["5+"] = 0
            summarized["5+"] += v
        else:
            summarized[k] = v

    logger.info(f"Number of units per building: {json.dumps(summarized, indent=2)}")

    num_units = list(summarized.keys())
    building_counts = list(summarized.values())

    plt.figure(figsize=(8, 8))
    plt.pie(
        building_counts,
        labels=num_units,
        autopct="%1.1f%%",
        startangle=140,
        colors=plt.cm.Paired.colors,
    )
    plt.axis("equal")
    plt.title(f"Building Count by Units (Total: {len(buildings)})")
    plt.legend(
        title="Number of Units", loc="center left", bbox_to_anchor=(1, 0, 0.5, 1)
    )  # Position legend outside the pie chart
    plt.tight_layout()

    plt.show()

    return num_buildings_with_units


def calculate_stats(buildings: dict[str, Any]):
    """Calculate stats the old-fashioned way."""

    # buildings_by_country = get_buildings_by_country(buildings)
    # avg_size_by_country(buildings)
    # get_units_at_time(buildings, city="Rexburg", time="Su 11:00")
    get_unit_types(buildings)
    # get_num_units_per_building(buildings)


def get_buildings_from_json():
    """Get buildings from local json file."""
    logger.info("Reading buildings from buildings.json...")
    with open("buildings.json", "r") as f:
        buildings = json.load(f)

    # buildings = clean_buildings(buildings)

    # # write cleaned buildings to file
    # with open("buildings_cleaned.json", "w", encoding="utf-8") as f:
    #     json.dump(buildings, f, ensure_ascii=False, indent=4)

    return buildings


def clean_buildings(buildings: dict[str, Any]):
    """Drop unnecessary fields"""
    logger.info("Dropping unnecessary fields...")
    for building in buildings:
        # drop match field from all buildings
        if "match" in building:
            del building["match"]
        # drop associated field from all buildings
        if "associated" in building:
            del building["associated"]
        # drop phones field from all buildings
        if "phones" in buildings:
            del building["phones"]
        # drop hours field from all buildings
        if "hours" in buildings:
            del building["hours"]

    logger.info("Done dropping fields.")
    return buildings


def get_buildings_from_web(update_json: bool = False):
    """Get buildings from web."""

    headers = {
        "Accept": "application/json",
        "Referer": "https://maps.churchofjesuschrist.org/",
    }

    base_url = (
        "https://maps.churchofjesuschrist.org/api/maps-proxy/v2/locations/identify"
    )
    NUM_BUILDINGS = 100_000
    layers = "MEETINGHOUSE"
    filters = ""
    associated = "WARDS"
    coordinates = "0,0"

    logger.info("Sending request to get buildings...")
    response = requests.get(
        f"{base_url}?layers={layers}&filters={filters}&associated={associated}&coordinates={coordinates}&nearest={NUM_BUILDINGS}",  # noqa: E501
        # cookies=cookies,
        headers=headers,
    )

    buildings = response.json()
    if update_json:
        write_buildings_to_json(buildings)
    return buildings


def write_buildings_to_json(buildings: dict[str, Any]):
    """write buildings to json file."""
    logger.info("Writing buildings to buildings.json...")
    with open("buildings.json", "w") as f:
        json.dump(buildings, f, indent=4)


if __name__ == "__main__":
    main()
