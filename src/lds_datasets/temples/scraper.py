"""Temple attendance scraper.

https://www.churchofjesuschrist.org/temples/schedule/appointment
Scrape temple capacity and reservations and save it to a json file.
Temple reservations site requires login.
"""
import json
import logging
import random
import sys
import time
from datetime import datetime
from multiprocessing.pool import ThreadPool
from typing import Any, Literal

import click
import requests
import structlog
from pydantic import BaseModel, NonNegativeInt, SecretStr, model_validator

logger = structlog.get_logger()

NUM_THREADS = 2

APPT_TYPE = Literal[
    "PROXY_BAPTISM", "PROXY_INITIATORY", "PROXY_ENDOWMENT", "PROXY_SEALING"
]

HEADERS = {
    "Accept": "application/json, text/plain, */*",
    "Content-Type": "application/json;charset=UTF-8",
    "Connection": "keep-alive",
    "Origin": "https://tos.churchofjesuschrist.org",
    "Referer": "https://tos.churchofjesuschrist.org/?locale=en&noCache=1692496499915",
    "Sec-Fetch-Dest": "empty",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Site": "same-origin",
    "User-agent": "Temple Scheduling Bot 1.0",
}


def login(username: str, password: SecretStr) -> requests.Session:
    """Login and set cookie for the returned Session."""
    session = requests.Session()
    # make initial request to get JSESSIONID, okta_state_token
    response = session.get("https://id.churchofjesuschrist.org/signin/")
    session.cookies.update(response.cookies)

    json_data = {
        "username": username,
    }

    response = session.post(
        "https://id.churchofjesuschrist.org/api/v1/authn",
        json=json_data,
    )
    json = response.json()
    okta_state_token = json["stateToken"]
    session.cookies.update(response.cookies)

    response = session.post(
        "https://id.churchofjesuschrist.org/api/v1/authn/factors/password/verify?rememberDevice=false",
        json={
            "password": password,
            "stateToken": okta_state_token,
        },
    )
    json = response.json()
    session.cookies.update(response.cookies)

    return session


@click.command()
@click.option("--username", help="LDS Account Username")
@click.option("--password", help="LDS Account Password")
def main(username: str, password: SecretStr):
    """Main function."""
    session = login(username=username, password=password)

    # right now, there are 174 temples, 4 ordinance types, and ~12 days/month to check
    # so we have 8352 getSessionInfo requests to make, in addition to
    # the 174 requests for each temple's meta info and 174 getTempleMonthlySchedule requests
    # that's a total of ~8700 requests for each month
    # TODO add click and subcommand to run for single day (need to figure out when to run it...)
    # TODO add to crontab to run nightly

    # TODO
    # data to capture
    # - monthly capacity, reservations
    # - monthly days with available ordinances, by ordinance type, by temple
    # - monthly sessions with available ordinances, by ordinance type, by temple
    # - all of this data by country, state (in the USA)
    # - eventually, all of this data for a given year
    # - a fantastic number would be -- last year N (total, male, female) members participated
    #   in Y sessions, by country, state, temple

    # read in all temple ids
    temple_org_ids = read_temple_org_ids_json()
    logger.info(f"Temples with scheduling available: {len(temple_org_ids)}")

    month, year = get_zero_indexed_search_month_and_year()

    # get the temple's meta info
    enriched_temples_info: list[Any] = []
    temple_scheduling: dict[str, Any] = {}

    pool = ThreadPool(processes=NUM_THREADS)

    logger.info(
        f"Searching for appointments in {year}_{month} for {len(temple_org_ids)} temples..."
    )
    temple_meta_info_inputs: list[tuple[requests.Session, str]] = []
    for temple_org_id in temple_org_ids:
        temple_meta_info_inputs.append((session, temple_org_id))
    temple_meta_infos = pool.starmap(get_temple_meta_info, temple_meta_info_inputs)
    logger.info(f"Retrieved temple meta info for {len(temple_meta_infos)} temples.")
    temple_org_id_to_name: dict[str, str] = {}
    for temple_meta_info in temple_meta_infos:
        t_org_id = temple_meta_info["templeOrgId"]
        t_name = temple_meta_info["templeName"]
        temple_org_id_to_name[t_org_id] = t_name

    get_session_data_inputs: list[tuple[requests.Session, str, int, int, int, str]] = []
    get_temple_ord_days_inputs: list[tuple[requests.Session, str, int, int, str]] = []
    for temple_meta_info in temple_meta_infos:
        temple_org_id = temple_meta_info["templeOrgId"]
        if "appointmentTypes" not in temple_meta_info:
            enriched_temples_info.append(temple_meta_info)
            continue
        temple_name = temple_meta_info["templeName"]
        if temple_name not in temple_scheduling:
            temple_scheduling[temple_name] = {}

        for appointment_type in temple_meta_info["appointmentTypes"]:
            if appointment_type not in temple_scheduling[temple_name]:
                temple_scheduling[temple_name][appointment_type] = {}
            get_temple_ord_days_inputs.append(
                (session, temple_org_id, year, month, appointment_type)
            )
    logger.info(
        "Retrieving available ordinance days...",
        num_inputs=len(get_temple_ord_days_inputs),
    )
    get_temple_ord_days_results = pool.starmap(
        get_temple_ord_days, get_temple_ord_days_inputs
    )

    for res in get_temple_ord_days_results:
        # unpack the returned data
        days, temple_org_id, year, month, appointment_type = res
        for day in days:
            temple_name = temple_org_id_to_name[temple_org_id]
            if day not in temple_scheduling[temple_name][appointment_type]:
                temple_scheduling[temple_name][appointment_type][day] = {}

            # build list of tuples for parameters
            get_session_data_inputs.append(
                (session, temple_org_id, year, month, day, appointment_type)
            )

    logger.info("Retrieving session data...", num_inputs=len(get_session_data_inputs))
    session_datas = pool.starmap(get_session_data, get_session_data_inputs)

    for session_data in session_datas:
        # unpack the returned data
        (
            daily_session_data,
            temple_org_id,
            year,
            month,
            day,
            appointment_type,
        ) = session_data
        temple_name = temple_org_id_to_name[temple_org_id]
        temple_scheduling[temple_name][appointment_type][day] = {
            "capacity": daily_session_data.capacity,
            "online_capacity": daily_session_data.online_capacity,
            "reserved": daily_session_data.reserved,
            "reserved_m": daily_session_data.reserved_m,
            "reserved_f": daily_session_data.reserved_f,
            "sessions": daily_session_data.sessions,
            "scraped_at": datetime.now().isoformat(),
        }

    monthly_data: dict[str, dict[str, dict[str, int]]] = {}
    # calculate the monthly capacity, reservations, days with available ordinances, and sessions
    for temple_name, ordinances in temple_scheduling.items():
        for appointment_type, data in ordinances.items():
            monthly_ord_capacity = 0
            monthly_ord_online_capacity = 0
            monthly_ord_reserved = 0
            monthly_ord_reserved_m = 0
            monthly_ord_reserved_f = 0
            monthly_days_with_available_ordinances = 0
            monthly_sessions = 0
            for day, day_data in data.items():
                monthly_ord_capacity += day_data["capacity"]
                monthly_ord_online_capacity += day_data["online_capacity"]
                monthly_ord_reserved += day_data["reserved"]
                monthly_ord_reserved_m += day_data["reserved_m"]
                monthly_ord_reserved_f += day_data["reserved_f"]
                monthly_days_with_available_ordinances += 1
                monthly_sessions += day_data["sessions"]

            # add the data
            if temple_name not in monthly_data:
                monthly_data[temple_name] = {}
            if appointment_type not in monthly_data[temple_name]:
                monthly_data[temple_name][appointment_type] = {}
            if monthly_ord_reserved_m + monthly_ord_reserved_f != 0:
                ratio = (monthly_ord_reserved_m - monthly_ord_reserved_f) / (
                    monthly_ord_reserved_m + monthly_ord_reserved_f
                )
            else:
                ratio = 0
            monthly_data[temple_name][appointment_type] = {
                "monthly_capacity": monthly_ord_capacity,
                "monthly_online_capacity": monthly_ord_online_capacity,
                "monthly_reserved": monthly_ord_reserved,
                "monthly_reserved_m": monthly_ord_reserved_m,
                "monthly_reserved_f": monthly_ord_reserved_f,
                "monthly_days_with_available_ordinances": monthly_days_with_available_ordinances,
                "monthly_sessions": monthly_sessions,
                #### additional calculated fields
                "monthly_m_f_ratio": ratio,
            }

    pool.close()
    pool.join()

    # save the data to a json file
    with open("temples/temple_scheduling.json", "w") as f:
        json.dump(temple_scheduling, f, indent=4)

    with open("temples/monthly_data.json", "w") as f:
        json.dump(monthly_data, f, indent=4)

    # calculate the total global capacity across all temples
    year_month = f"{year}_{month}"
    global_month_data: dict[str, dict[str, Any]] = {}
    if year_month not in global_month_data:
        global_month_data[year_month] = {}
    global_month_data[year_month]["temples"] = len(temple_org_ids)
    for _, ordinances in monthly_data.items():
        for ord_name, data in ordinances.items():
            # initialize dicts and values, if necessary
            if ord_name not in global_month_data[year_month]:
                global_month_data[year_month][ord_name] = {}
            if "monthly_capacity" not in global_month_data[year_month][ord_name]:
                global_month_data[year_month][ord_name]["monthly_capacity"] = 0
            if "monthly_online_capacity" not in global_month_data[year_month][ord_name]:
                global_month_data[year_month][ord_name]["monthly_online_capacity"] = 0
            if "monthly_reserved" not in global_month_data[year_month][ord_name]:
                global_month_data[year_month][ord_name]["monthly_reserved"] = 0
            if "monthly_reserved_m" not in global_month_data[year_month][ord_name]:
                global_month_data[year_month][ord_name]["monthly_reserved_m"] = 0
            if "monthly_reserved_f" not in global_month_data[year_month][ord_name]:
                global_month_data[year_month][ord_name]["monthly_reserved_f"] = 0
            if (
                "monthly_days_with_available_ordinances"
                not in global_month_data[year_month][ord_name]
            ):
                global_month_data[year_month][ord_name][
                    "monthly_days_with_available_ordinances"
                ] = 0
            if "monthly_sessions" not in global_month_data[year_month][ord_name]:
                global_month_data[year_month][ord_name]["monthly_sessions"] = 0

            # add the data
            global_month_data[year_month][ord_name]["monthly_capacity"] += data[
                "monthly_capacity"
            ]
            global_month_data[year_month][ord_name]["monthly_online_capacity"] += data[
                "monthly_online_capacity"
            ]
            global_month_data[year_month][ord_name]["monthly_reserved"] += data[
                "monthly_reserved"
            ]
            global_month_data[year_month][ord_name]["monthly_reserved_m"] += data[
                "monthly_reserved_m"
            ]
            global_month_data[year_month][ord_name]["monthly_reserved_f"] += data[
                "monthly_reserved_f"
            ]
            global_month_data[year_month][ord_name][
                "monthly_days_with_available_ordinances"
            ] += data["monthly_days_with_available_ordinances"]
            global_month_data[year_month][ord_name]["monthly_sessions"] += data[
                "monthly_sessions"
            ]

    # add in the calculated fields
    for ord_name, data in global_month_data[year_month].items():
        if ord_name == "temples":
            continue
        if data["monthly_reserved_m"] + data["monthly_reserved_f"] != 0:
            ratio = (data["monthly_reserved_m"] - data["monthly_reserved_f"]) / (
                data["monthly_reserved_m"] + data["monthly_reserved_f"]
            )
        else:
            ratio = 0
        data["monthly_m_f_ratio"] = ratio

    logger.info(f"{global_month_data}")
    with open("temples/global_month_data.json", "w") as f:
        json.dump(global_month_data, f, indent=4)


class DailySessionData(BaseModel):
    """Encapsulate data we retrieve for a day's sessions."""

    capacity: NonNegativeInt = 0
    online_capacity: NonNegativeInt = 0
    reserved: NonNegativeInt = 0
    reserved_m: NonNegativeInt = 0
    reserved_f: NonNegativeInt = 0
    sessions: NonNegativeInt = 0
    mismatch: bool = False

    @model_validator(mode="after")
    def ensure_reservation_count(self) -> "DailySessionData":
        """Ensure the m/f count sums to the total and is less than capacity."""
        if self.reserved_m + self.reserved_f != self.reserved:
            self.mismatch = True
        if self.reserved > self.capacity:
            self.mismatch = True
        return self


class SessionData(BaseModel):
    """Encapsulate a single session's data."""

    capacity: NonNegativeInt = 0
    online_capacity: NonNegativeInt = 0
    reserved: NonNegativeInt = 0
    reserved_m: NonNegativeInt = 0
    reserved_f: NonNegativeInt = 0
    mismatch: bool = False

    @model_validator(mode="after")
    def ensure_reservation_count(self) -> "SessionData":
        """Ensure the m/f count sums to the total and is less than capacity."""
        if self.reserved_m + self.reserved_f != self.reserved:
            self.mismatch = True
        if self.reserved > self.capacity:
            self.mismatch = True
        return self


def get_session_data(
    session: requests.Session,
    temple_org_id: str,
    year: int,
    month: int,
    day: int,
    appointment_type: APPT_TYPE,
) -> tuple[DailySessionData, str, int, int, int, APPT_TYPE]:
    """Get the SessionData for a specific appointment type on a specific date.

    Args:
    - temple_org_id: the temple's org id
    - year: the year
    - month: the month (0-indexed, e.g. 0 = January, 11 = December)
    - day: the day of the month
    - appointment_type: the appointment type
        i.e. PROXY_BAPTISM, PROXY_INITIATORY, PROXY_ENDOWMENT, PROXY_SEALING

    Returns:
    - The DailySessionData for the day's sessions
    - The input args are returned, so we can use this in a ThreadPool
    """
    json_data = {
        "sessionYear": year,
        "sessionMonth": month,
        "sessionDay": day,
        "appointmentType": appointment_type,
        "templeOrgId": temple_org_id,
        "isGuestConfirmation": False,
    }

    logger.debug(
        "Retrieving session info...",
        temple_org_id=temple_org_id,
        year=year,
        month=month,
        day=day,
        appt_type=appointment_type,
    )
    response = send_request_with_retry(
        session=session,
        url="https://tos.churchofjesuschrist.org/api/templeSchedule/getSessionInfo",
        method="POST",
        json=json_data,
    )

    total_daily_capacity = 0
    total_daily_online_capacity = 0
    total_daily_reserved = 0
    total_daily_reserved_m = 0
    total_daily_reserved_f = 0
    data = {}
    if response is None:
        logger.error(
            f"Unable to retrieve session info for {temple_org_id} {year}-{month}-{day} {appointment_type}"
        )
        return (DailySessionData(), temple_org_id, year, month, day, appointment_type)
    data = response.json()
    if "sessionList" not in data:
        logger.error(
            f"Unable to retrieve session info for {temple_org_id} {year}-{month}-{day} {appointment_type}"
        )
        return (DailySessionData(), temple_org_id, year, month, day, appointment_type)
    for session in data["sessionList"]:
        match appointment_type:
            case "PROXY_BAPTISM":
                session_data = get_baptism_session_data(session)
                total_daily_capacity += session_data.capacity
                total_daily_online_capacity += session_data.online_capacity
                total_daily_reserved += session_data.reserved
                total_daily_reserved_m += session_data.reserved_m
                total_daily_reserved_f += session_data.reserved_f
            case "PROXY_INITIATORY":
                session_data = get_initiatory_session_data(session)
                total_daily_capacity += session_data.capacity
                total_daily_online_capacity += session_data.online_capacity
                total_daily_reserved += session_data.reserved
                total_daily_reserved_m += session_data.reserved_m
                total_daily_reserved_f += session_data.reserved_f
            case "PROXY_ENDOWMENT":
                session_data = get_endowment_session_data(session)
                total_daily_capacity += session_data.capacity
                total_daily_online_capacity += session_data.online_capacity
                total_daily_reserved += session_data.reserved
                total_daily_reserved_m += session_data.reserved_m
                total_daily_reserved_f += session_data.reserved_f
            case "PROXY_SEALING":
                session_data = get_sealing_session_data(session)
                total_daily_capacity += session_data.capacity
                total_daily_online_capacity += session_data.online_capacity
                total_daily_reserved += session_data.reserved
                total_daily_reserved_m += session_data.reserved_m
                total_daily_reserved_f += session_data.reserved_f

    dsd = DailySessionData(
        capacity=total_daily_capacity,
        online_capacity=total_daily_online_capacity,
        reserved=total_daily_reserved,
        reserved_m=total_daily_reserved_m,
        reserved_f=total_daily_reserved_f,
        sessions=len(data["sessionList"]),
    )
    if dsd.mismatch:
        logger.info(
            "Daily session data mismatch.",
            temple_org_id=temple_org_id,
            year=year,
            month=month,
            day=day,
            appt_type=appointment_type,
            daily_session_data=dsd,
        )
    return (dsd, temple_org_id, year, month, day, appointment_type)


def get_baptism_session_data(session: dict[str, Any]) -> SessionData:
    """Calculate the session data for a baptism session."""
    sd = SessionData()
    details = session["details"]
    if "capacity" in details:
        sd.capacity = details["capacity"]
    if "onlineCapacity" in details:
        sd.online_capacity = details["onlineCapacity"]
    if "reserved" in details:
        sd.reserved = details["reserved"]
    if "totalFemalePatronCount" in details:
        sd.reserved_f = details["totalFemalePatronCount"]
    if "totalMalePatronCount" in details:
        sd.reserved_m = details["totalMalePatronCount"]

    # trigger validation
    ret_sd = SessionData(**sd.model_dump())
    if ret_sd.mismatch:
        logger.debug(
            f"Session data mismatch for {session['time']} {session['appointmentType']}: {sd}"
        )
    return ret_sd


def get_initiatory_session_data(session: dict[str, Any]) -> SessionData:
    """Calculate the session data for an initiatory session.""" ""
    sd = SessionData()
    details = session["details"]
    if "maleCapacity" in details and "femaleCapacity" in details:
        sd.capacity = details["maleCapacity"] + details["femaleCapacity"]
    if "onlineCapacity" in details:
        sd.online_capacity = details["onlineCapacity"]
    if "reserved" in details:
        sd.reserved = details["reserved"]
    if "totalFemalePatronCount" in details:
        sd.reserved_f = details["totalFemalePatronCount"]
    if "totalMalePatronCount" in details:
        sd.reserved_m = details["totalMalePatronCount"]

    # trigger validation
    ret_sd = SessionData(**sd.model_dump())
    if ret_sd.mismatch:
        logger.debug(
            f"Session data mismatch for {session['time']} {session['appointmentType']}: {sd}"
        )
    return ret_sd


def get_endowment_session_data(session: dict[str, Any]) -> SessionData:
    sd = SessionData()
    details = session["details"]
    if "capacity" in details:
        sd.capacity = details["capacity"]
    if "onlineCapacity" in details:
        sd.online_capacity = details["onlineCapacity"]
    if "reserved" in details:
        sd.reserved = details["reserved"]
    if "totalFemalePatronCount" in details:
        sd.reserved_f = details["totalFemalePatronCount"]
    if "totalMalePatronCount" in details:
        sd.reserved_m = details["totalMalePatronCount"]

    # trigger validation
    ret_sd = SessionData(**sd.model_dump())
    if ret_sd.mismatch:
        logger.debug(
            f"Session data mismatch for {session['time']} {session['appointmentType']}: {sd}"
        )
    return ret_sd


def get_sealing_session_data(session: dict[str, Any]) -> SessionData:
    sd = SessionData()
    details = session["details"]
    if "capacity" in details:
        sd.capacity = details["capacity"]
    if "onlineCapacity" in details:
        sd.online_capacity = details["onlineCapacity"]
    if "reserved" in details:
        sd.reserved = details["reserved"]
    if "totalFemalePatronCount" in details:
        sd.reserved_f = details["totalFemalePatronCount"]
    if "totalMalePatronCount" in details:
        sd.reserved_m = details["totalMalePatronCount"]

    # trigger validation
    ret_sd = SessionData(**sd.model_dump())
    if ret_sd.mismatch:
        logger.debug(
            f"Session data mismatch for {session['time']} {session['appointmentType']}: {sd}"
        )
    return ret_sd


def get_temple_meta_info(
    session: requests.Session, temple_org_id: str
) -> dict[str, Any]:
    """Get the temple's metadata info."""
    # set the temple to this temple_org_id
    json_data = {
        "orgId": temple_org_id,
    }

    logger.debug("Retrieving temple meta info...", temple_org_id=temple_org_id)
    response = send_request_with_retry(
        session=session,
        url="https://tos.churchofjesuschrist.org/api/templeInfo/setTemple",
        method="POST",
        json=json_data,
    )
    if response is None:
        logger.error(f"Unable to retrieve temple meta info for {temple_org_id}")
        return {}
    temple_info = response.json()

    return temple_info


def get_zero_indexed_search_month_and_year() -> tuple[int, int]:
    """Get the current month and year, zero indexed."""
    # being explicit
    # This is getting the current month, 0-indexed,
    # then adding 1 to check the next month
    # e.g. If it's currently November, 11 - 1 = 10, then 10 + 1 = 11 and we'd check December
    month = (datetime.now().month - 1) + 1
    year = datetime.now().year
    # the months are 0-indexed. so 0 is january, 1 is february, 11 is december.
    if month == 12:
        month = 0
        year = datetime.now().year + 1

    if month < 0 or month >= 12:
        raise ValueError(f"Invalid month: {month}")

    return month, year


def get_temple_ord_days(
    session: requests.Session,
    temple_org_id: str,
    year: int,
    month: int,
    appt_type: APPT_TYPE,
) -> tuple[list[int], str, int, int, APPT_TYPE]:
    """Get the monthly schedule for a temple.

    Args:
    - temple_org_id: the temple's org id

    Returns:
    - days: a list of days in the month that have scheduling available for the appt_type
    - The input args are returned, so we can use this in a ThreadPool
    """
    json_data = {
        "sessionDate": "2023-08-20T02:00:43.395Z",
        "month": month,
        "year": year,
        "templeOrgId": temple_org_id,
        "appointmentType": appt_type,
        # 'gender': 'MALE',
        # 'inclSelf': True,
        # 'addSpouse': False,
        # 'apptMales': 1,
        # 'apptFemales': 0,
        # 'isGroupAppt': False,
    }

    logger.debug(
        "Retrieving monthly ordinance days...",
        temple_org_id=temple_org_id,
        year=year,
        month=month,
        appt_type=appt_type,
    )
    response = send_request_with_retry(
        session=session,
        url="https://tos.churchofjesuschrist.org/api/templeSchedule/getTempleMonthlySchedule",
        method="POST",
        json=json_data,
    )
    if response is None:
        logger.error(
            f"Unable to retrieve monthly schedule.",
            temple_org_id=temple_org_id,
            year=year,
            month=month,
            appt_type=appt_type,
        )
        return ([], temple_org_id, year, month, appt_type)
    data = response.json()

    ordinance_days: list[int] = []
    if "days" in data:
        for day in data["days"]:
            if "templeOpen" in day and "ordinanceAvailable" in day:
                if day["templeOpen"] and day["ordinanceAvailable"]:
                    ordinance_days.append(day["dayOfMonth"])
    return (ordinance_days, temple_org_id, year, month, appt_type)


def read_temple_org_ids_json():
    """Read in the temple ids from a json file.

    TODO: automate populating this file. For now, I just manually downloaded from
    https://tos.churchofjesuschrist.org/api/templeConfig/findAllOnlineSchedulingStatuses

    JSON file is formatted like:
    [
        {
            "templeOrgId": 75531,
            "onlineSchedulingAvailable": True
        },
        {
            "templeOrgId": 4004669,
            "onlineSchedulingAvailable": True
        },
        ...
    ]
    """
    all_temples: list[dict[str, str]] = []
    with open("./temples/all_temple_ids.json", "r") as f:
        all_temples = json.load(f)

    temple_ids: list[str] = []
    for temple in all_temples:
        if (
            "onlineSchedulingAvailable" not in temple
            or temple["onlineSchedulingAvailable"] is False
        ):
            logger.info(
                "No scheduling for temple.", temple_org_id=temple["templeOrgId"]
            )
        else:
            temple_ids.append(temple["templeOrgId"])

    return temple_ids


def send_request_with_retry(
    session: requests.Session,
    url: str,
    method: str,
    json: dict[str, Any],
    max_retries: int = 3,
):
    # We randomize the retry delay, to avoid all threads retrying at the same time
    retry_delay = random.uniform(0.1, 0.5)
    retries = 0

    while retries < max_retries:
        try:
            response = session.request(
                url=url, method=method, json=json, headers=HEADERS
            )
        except requests.exceptions.ConnectionError:
            logger.info(f"Connection error at {url}. Retrying...")
            time.sleep(retry_delay)
            retry_delay *= 2  # Exponential backoff
            retries += 1
            continue

        match response.status_code:
            case 429:
                # Server is rate-limiting the requests
                logger.info(f"Received a 429 response at {url}.")
                logger.info(f"Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
                retry_delay *= 2  # Exponential backoff
                retries += 1
            case 200:
                # sometimes the server responds with a 200, but it's because we've been
                # redirected to the login page again, handle that and retry
                try:
                    _ = response.json()
                    return response
                except requests.JSONDecodeError:
                    if (
                        "<!DOCTYPE html" in response.text
                        and "id.churchofjesuschrist.org" in response.text
                        and "runLoginPage" in response.text
                    ):
                        logger.info("Redirected to login again. Retrying...")
                        # login_set_cookie_info()
            case _:
                return response

    logger.error(f"Max retries reached. Request to {url} failed.")
    return None


if __name__ == "__main__":
    logging.basicConfig(format="%(message)s", stream=sys.stdout, level=logging.INFO)

    logger.info(f"Starting temple scraper using {NUM_THREADS} threads...")
    start_time = datetime.now()
    main()
    end_time = datetime.now()
    logger.info(f"Finished temple scraper.", duration=end_time - start_time)
