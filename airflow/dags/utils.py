"""Файл с функциями для работы с API SpaceX"""
import time
import logging
from entities import (
    Capsules,
    Cores,
    Crew,
    Landpads,
    LaunchesSpaceX,
    Launchpads,
    Payload,
    Rockets,
    Ships,
    StarlinkSat,
)
from urllib3.exceptions import ReadTimeoutError, MaxRetryError

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

import json
import logging
import time
import requests

import requests.packages.urllib3.util.connection as urllib3_cn
import socket

urllib3_cn.allowed_gai_family = lambda: socket.AF_INET

logger = logging.getLogger(__name__)

def get_data_from_url(url: str, max_attempts=3):
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36',
        'Accept': 'application/json',
        'Accept-Encoding': 'gzip, deflate, br, zstd',
        'Connection': 'keep-alive'
    }

    for attempt in range(max_attempts):
        try:
            timeout = (15, 15 + 15 * attempt)   # 60, 120, 180
            logger.info(f"Попытка {attempt+1}/{max_attempts} к {url} (таймаут read: {timeout[1]}с)")
            response = requests.get(
                url,
                headers=headers,
                timeout=timeout,
                stream=True
                # hooks={'pre_request': lambda r: setattr(r, 'socket_family', socket.AF_INET)}
            )
            response.raise_for_status()

            content = b""
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    content += chunk
                    logger.debug(f"Скачано {len(content)/1024/1024:.1f} MB")

            data = json.loads(content.decode('utf-8'))
            logger.info(f"Успешно получено {len(content)/1024/1024:.1f} MB (сжато)")
            return data

        except requests.exceptions.RequestException as e:
            if attempt == max_attempts - 1:
                raise ValueError(f"Не удалось получить данные от {url} после {max_attempts} попыток: {str(e)}")
            logger.warning(f"Попытка {attempt+1} провалилась: {str(e)}. Ждём 15 сек...")
            time.sleep(15)

    raise RuntimeError("Не должно сюда дойти")


def get_data_from_query(endpoint: str, limit: int = 1000, page: int = 1, extra_query: dict = None):
    """Универсальная функция для тяжёлых эндпоинтов через POST /query"""
    url = f"https://api.spacexdata.com/v4/{endpoint}/query"
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Accept': 'application/json',
        'Accept-Encoding': 'gzip, deflate, br',
        'Content-Type': 'application/json'
    }

    payload = {
        "query": extra_query or {},           # например {"upcoming": True} для launches
        "options": {
            "limit": limit,
            "page": page,
            "sort": {"date_utc": -1} if endpoint == "launches" else {"spaceTrack.EPOCH": -1}
        }
    }

    for attempt in range(3):
        try:
            logger.info(f"POST /query {endpoint} (page {page}, limit {limit}) попытка {attempt+1}")
            response = requests.post(url, json=payload, headers=headers, timeout=(10, 60))
            response.raise_for_status()
            data = response.json()
            docs = data.get("docs", [])
            logger.info(f"Получено {len(docs)} записей из {endpoint}")
            return docs
        except Exception as e:
            if attempt == 2:
                raise
            logger.warning(f"Попытка {attempt+1} провалилась: {e}")
            time.sleep(5)

def get_all_from_query(endpoint: str, batch_size=100):
    all_docs = []
    page = 1
    while True:
        docs = get_data_from_query(endpoint, limit=batch_size, page=page)
        if not docs:
            break
        all_docs.extend(docs)
        logger.info(f"Собрано {len(all_docs)} записей из {endpoint} (page {page})")
        page += 1
        time.sleep(2)  # пауза, чтобы не нагружать API
    return all_docs

def get_capsules(sat_json):
    """Функция для работы с данными запусков SpaceX"""
    capsules = Capsules(
        reuse_count=sat_json["reuse_count"],
        water_landings=sat_json["water_landings"],
        land_landings=sat_json["land_landings"],
        last_update=sat_json["last_update"],
        launches=sat_json["launches"],
        serial=sat_json["serial"],
        status=sat_json["status"],
        type=sat_json["type"],
        id=sat_json["id"],
    )
    return capsules


def get_cores(sat_json):
    """Функция получения данных об объекстах SpaceX вернувшихся на землю"""
    cores = Cores(
        block=sat_json["block"],
        reuse_count=sat_json["reuse_count"],
        rtls_attempts=sat_json["rtls_attempts"],
        rtls_landings=sat_json["rtls_landings"],
        asds_attempts=sat_json["asds_attempts"],
        asds_landings=sat_json["asds_landings"],
        last_update=sat_json["last_update"],
        launches=sat_json["launches"],
        serial=sat_json["serial"],
        status=sat_json["status"],
        id=sat_json["id"],
    )
    return cores


def get_crew(sat_json):
    """Функция получения данных о носителях Crew"""
    crew = Crew(
        name=sat_json["name"],
        agency=sat_json["agency"],
        image=sat_json["image"],
        wikipedia=sat_json["wikipedia"],
        launches=sat_json["launches"],
        status=sat_json["status"],
        id=sat_json["id"],
    )
    return crew


def get_landpads(sat_json):
    """Функция получения данных о площадках приземления"""
    landpads = Landpads(
        name=sat_json["name"],
        full_name=sat_json["full_name"],
        status=sat_json["status"],
        type=sat_json["type"],
        locality=sat_json["locality"],
        region=sat_json["region"],
        latitude=sat_json["latitude"],
        longitude=sat_json["longitude"],
        landing_attempts=sat_json["landing_attempts"],
        landing_successes=sat_json["landing_successes"],
        wikipedia=sat_json["wikipedia"],
        details=sat_json["details"],
        launches=sat_json["launches"],
        id=sat_json["id"],
    )
    return landpads


def get_launchpads(sat_json):
    """Функция получения данных о площадках приземления"""
    launchpads = Launchpads(
        name=sat_json["name"],
        full_name=sat_json["full_name"],
        locality=sat_json["locality"],
        region=sat_json["region"],
        timezone=sat_json["timezone"],
        latitude=sat_json["latitude"],
        longitude=sat_json["longitude"],
        launch_attempts=sat_json["launch_attempts"],
        launch_successes=sat_json["launch_successes"],
        rockets=sat_json["rockets"],
        launches=sat_json["launches"],
        status=sat_json["status"],
        id=sat_json["id"],
    )
    return launchpads


def get_payload(sat_json):
    """Функция получения данных о стартовых площадках"""
    payload = Payload(
        dragon=sat_json["dragon"],
        name=sat_json["name"],
        type=sat_json["type"],
        reused=sat_json["reused"],
        launch=sat_json["launch"],
        customers=sat_json["customers"],
        norad_ids=sat_json["norad_ids"],
        nationalities=sat_json["nationalities"],
        manufacturers=sat_json["manufacturers"],
        mass_kg=sat_json["mass_kg"],
        mass_lbs=sat_json["mass_lbs"],
        orbit=sat_json["orbit"],
        reference_system=sat_json["reference_system"],
        regime=sat_json["regime"],
        longitude=sat_json["longitude"],
        semi_major_axis_km=sat_json["semi_major_axis_km"],
        eccentricity=sat_json["eccentricity"],
        periapsis_km=sat_json["periapsis_km"],
        apoapsis_km=sat_json["apoapsis_km"],
        inclination_deg=sat_json["inclination_deg"],
        period_min=sat_json["period_min"],
        lifespan_years=sat_json["lifespan_years"],
        epoch=sat_json["epoch"],
        mean_motion=sat_json["mean_motion"],
        raan=sat_json["raan"],
        arg_of_pericenter=sat_json["arg_of_pericenter"],
        mean_anomaly=sat_json["mean_anomaly"],
        id=sat_json["id"],
    )
    return payload


def get_ships(sat_json):
    """Функция получения данных о космических кораблях"""
    ships = Ships(
        legacy_id=sat_json["legacy_id"],
        model=sat_json["model"],
        type=sat_json["type"],
        roles=sat_json["roles"],
        imo=sat_json["imo"],
        mmsi=sat_json["mmsi"],
        abs=sat_json["abs"],
        class_ship=sat_json["class"],
        mass_kg=sat_json["mass_kg"],
        mass_lbs=sat_json["mass_lbs"],
        year_built=sat_json["year_built"],
        home_port=sat_json["home_port"],
        status=sat_json["status"],
        speed_kn=sat_json["speed_kn"],
        course_deg=sat_json["course_deg"],
        latitude=sat_json["latitude"],
        longitude=sat_json["longitude"],
        last_ais_update=sat_json["last_ais_update"],
        link=sat_json["link"],
        image=sat_json["image"],
        launches=sat_json["launches"],
        name=sat_json["name"],
        active=sat_json["active"],
        id=sat_json["id"],
    )
    return ships


def get_rockets(sat_json):
    """Функция получения данных о ракетоносителях"""
    rockets = Rockets(
        height=sat_json["height"],
        diameter=sat_json["diameter"],
        mass=sat_json["mass"],
        first_stage=sat_json["first_stage"],
        second_stage=sat_json["second_stage"],
        engines=sat_json["engines"],
        landing_legs=sat_json["landing_legs"],
        payload_weights=sat_json["payload_weights"],
        flickr_images=sat_json["flickr_images"],
        name=sat_json["name"],
        type=sat_json["type"],
        active=sat_json["active"],
        stages=sat_json["stages"],
        boosters=sat_json["boosters"],
        cost_per_launch=sat_json["cost_per_launch"],
        success_rate_pct=sat_json["success_rate_pct"],
        first_flight=sat_json["first_flight"],
        country=sat_json["country"],
        company=sat_json["company"],
        wikipedia=sat_json["wikipedia"],
        description=sat_json["description"],
        id=sat_json["id"],
    )
    return rockets

def get_launches(sat_json):
    """Функция для работы с данными запусков SpaceX"""
    launches = LaunchesSpaceX(
        fairings=sat_json["fairings"],
        links=sat_json["links"],
        static_fire_date_utc=sat_json["static_fire_date_utc"],
        static_fire_date_unix=sat_json["static_fire_date_unix"],
        tbd=sat_json["tbd"],
        net=sat_json["net"],
        window=sat_json["window"],
        rocket=sat_json["rocket"],
        success=sat_json["success"],
        failures=sat_json["failures"],
        details=sat_json["details"],
        crew=sat_json["crew"],
        ships=sat_json["ships"],
        capsules=sat_json["capsules"],
        payloads=sat_json["payloads"],
        launchpad=sat_json["launchpad"],
        auto_update=sat_json["auto_update"],
        flight_number=sat_json["flight_number"],
        name=sat_json["name"],
        date_utc=sat_json["date_utc"],
        date_unix=sat_json["date_unix"],
        date_local=sat_json["date_local"],
        date_precision=sat_json["date_precision"],
        upcoming=sat_json["upcoming"],
        cores=sat_json["cores"],
        id=sat_json["id"],
    )
    return launches

def get_starlinks(sat_json):
    """Функция для работы с данными спутников Starlink"""
    starlink_sat = StarlinkSat(
        spacetrack=sat_json["spaceTrack"],
        version=sat_json["version"],
        launch=sat_json["launch"],
        longitude=sat_json["longitude"],
        latitude=sat_json["latitude"],
        height_km=sat_json["height_km"],
        velocity_kms=sat_json["velocity_kms"],
        id=sat_json["id"],
    )
    return starlink_sat