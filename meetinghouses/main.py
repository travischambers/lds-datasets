"""Meetinghouse scraper."""

# Scrape all meetinghouses from https://maps.churchofjesuschrist.org/
# and save them to a JSON file.

import json
from typing import Any

import requests
import structlog

logger = structlog.get_logger()


def main():
    """Main function."""

    # scrape all the meetinghouses
    # buildings = get_buildings_from_json()
    buildings = get_buildings_from_web()
    logger.info(f"Total buildings found: {len(buildings)}")

    # calculate some stats
    associated = []
    types = set()
    count_of_building_with_no_units = 0
    for building in buildings:
        if "associated" not in building:
            count_of_building_with_no_units += 1
            continue
        for assoc in building["associated"]:
            associated.append(assoc)
            types.add(assoc["type"])

    logger.info(f"Total units: {len(associated)}")
    logger.info(f"Total unit types: {len(types)}")
    logger.info(f"Total buildings with no units: {count_of_building_with_no_units}")

    # print number of associated by type
    type_counts = {}
    for t in types:
        count = len([a for a in associated if a["type"] == t])
        type_counts[t] = count

    logger.info(f"Unit by type: {json.dumps(type_counts, indent=2)}")


def get_buildings_from_json():
    """Get buildings from local json file."""
    logger.info("Reading buildings from buildings.json...")
    with open("buildings.json", "r") as f:
        buildings = json.load(f)
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
    NUM_BUILDINGS = 100000
    # NUM_BUILDINGS = 100_000
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
