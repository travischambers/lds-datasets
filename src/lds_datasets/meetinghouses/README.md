# Getting Started

This project uses python 3.10 and poetry.

- Install: [https://python-poetry.org/docs/#installation](https://python-poetry.org/docs/#installation)
- Setup: `poetry shell`, `poetry install`
- Run: `python meetinghouses/main.py`

The `calculate_stats` method contains calls to different analyses. They can be selected by manually commenting/uncommenting different methods.

# Data

The data contained in `buildings.json` was scraped from [https://maps.churchofjesuschrist.org/](https://maps.churchofjesuschrist.org/). It was last updated on August 2nd, 2023.

The data can be re-fetched via the `get_buildings_from_web` method in `meetinghouses/main.py`.

# Stats

While `buildings.json` contains the raw data and much more analysis could be done, I've written a few methods to calculate some stats.

## Buildings

- Total buildings: 19,322
- Bulidings with 0 units (wards or branches): 568
- Buildings with 1 unit: 11,431
- Buildings with 2 units: 4270
- Buildings with 3 units: 2332
- Buildings with 4+ units: 721
- Buildings with no address: 92

Other analysis could still be performed here to find the number of units per building by country, state, or by building size,

## Units

- Total units (wards or branches): 30115

While other stats are calculated, the data is suspect. It wasn't clear to me how different unit types were defined. For example, a Unit Type of `WARD` exists, but also `WARD__ENGLISH`. With only 47 `WARD__ENGLISH`, I can only guess which wards get that tag and why.

# Contributing

PRs are welcome!
