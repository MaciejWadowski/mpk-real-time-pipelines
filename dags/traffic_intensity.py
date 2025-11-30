import requests
from typing import Any
import time
from math import ceil
import datetime
from pendulum import DateTime

BATCH_SIZE = 100
STOPS_INCLUDED = set([
    "Muzeum Narodowe",
    "Jubilat",
    "Nowy Kleparz",
    "Rondo Grunwaldzkie",
    "Konopnickiej",
    "AGH / UR",
    "Plac Inwalidów",
    "Rondo Matecznego",
    "Kamieńskiego",
    "Prokocim Szpital",
    "Opolska Estakada",
    "Czarnowiejska",
    "Bronowice SKA",
    "Politechnika",
    "Kamieńskiego Wiadukt",
    "Bieżanowska",
    "Biskupa Prandoty",
    "Bonarka",
    "Ludwinów",
    "Dworzec Główny Zachód",
    "Makowskiego",
    "Jerzmanowskiego",
    "UR al. 29 Listopada",
    "Rondo Barei",
    "Łobzów SKA",
    "Azory",
    "Radio Kraków",
    "Zajezdnia Wola Duchacka",
    "Rondo Ofiar Katynia",
    "Przybyszewskiego",
    "Kawiory",
    "Wola Duchacka",
    "Miasteczko Studenckie AGH",
    "Dobrego Pasterza",
    "Wlotowa",
    "Cmentarz Rakowicki Zachód",
    "Bujaka",
    "Os. Oświecenia",
    "Park Wodny",
    "Rondo Mogilskie",
    "Góra Borkowska",
    "Teatr Ludowy",
    "Mistrzejowice",
    "Rondo Młyńskie",
    "Różyckiego",
    "Makowa",
    "Malborska",
    "Ks. Meiera",
    "Wiślicka",
    "Miechowity",
    "Dworzec Główny Wschód",
    "Pilotów",
    "Łagiewniki",
    "Zarzecze",
    "Chopina",
    "Halszki",
    "Os. Podwawelskie",
    "Krowoderskich Zuchów",
    "Miśnieńska",
    "Os. Złotego Wieku",
    "Teatr Słowackiego",
    "Judyma",
    "Kapelanka",
    "Mackiewicza",
    "Biprostal",
    "Mazowiecka",
    "Marchołta",
    "Kurzei",
    "Łempickiego",
    "Słomiana",
    "Kobierzyńska",
    "Banacha",
    "Imbramowska",
    "Armii Krajowej",
    "Narzymskiego",
    "Surzyckiego",
    "Lindego",
    "Olsza II",
    "Stojałowskiego",
    "Os. Kurdwanów",
    "Pszenna",
    "Beskidzka",
    "Struga",
    "Rondo Piastowskie",
    "Sławka",
    "Rondo Hipokratesa",
    "UR Balicka",
    "Młynówka SKA",
    "Aleja Róż",
    "Godlewskiego"
    "Wieliczka Stacja Paliw",
    "Kąpielowa",
    "Wieliczka Miasto",
    "Wańkowicza",
    "Os. Mistrzejowice Nowe",
    "Fatimska",
    "Kliny Zacisze",
    "Struga",
    "Teligi",
    "Nowy Prokocim",
    "Nowosądecka",
    "Rondo Kocmyrzowskie im. Ks. Gorzelanego",
    "Podgórze SKA",
    "Conrada",
    "Os. Pod Fortem",
    "Lubicz",
    "Władysława Jagiełły",
    "Jana Kazimierza",
    "Leszka Białego",
    "Ćwiklińskiej",
    "Dunikowskiego",
    "Rondo Hipokratesa",
    "Wyki",
    "Majora",
    "Węgrzce A1",
    "Szwedzka",
    "Stoczniowców",
    "Powstańców Garaże",
    "Piaski Wielkie",
    "Cracovia Błonia",
    "Węgrzce Starostwo Powiatowe",
    "Witkowicka",
    "Arka",
    "Os. Jagiellońskie",
    "Chełm",
    "Węzeł Wielicki",
    "Teatr Słowackiego",
    "Szpital Jana Pawła II",
    "Stachiewicza",
    "Rondo Czyżyńskie",
    "Szpital Narutowicza",
    "Bratysławska",
    "Os. Zgody",
    "Piastowska",
    "Brodowicza",
    ]
)


def fetch_tomtom_traffic_intensity(lat: float, lon: float, api_key: str) -> dict[str, Any]:
    """
    Fetches traffic intensity from TomTom API for a given latitude and longitude. Retries up to 3 times on failure.
    """
    url = (
        f"https://api.tomtom.com/traffic/services/4/flowSegmentData/absolute/10/json"
        f"?point={lat},{lon}&unit=KMPH&key={api_key}"
    )
    for i in range(3):
        try:
            response = requests.get(url)
            response.raise_for_status()
            data = response.json()
            return {
                "current_speed": data['flowSegmentData']['currentSpeed'],
                "free_flow_speed": data['flowSegmentData']['freeFlowSpeed'],
                "current_travel_time": data['flowSegmentData']['currentTravelTime'],
                "free_flow_travel_time": data['flowSegmentData']['freeFlowTravelTime'],
                "confidence": data['flowSegmentData']['confidence']
            }
        except Exception as e:
            if i < 2:
                time.sleep(2 ** (i + 1))
            else:
                print(f"Failed to fetch TomTom data for ({lat}, {lon}): {e}")
                raise

def get_gtfs_stops_from_snowflake(logical_date: DateTime, hook) -> list[dict[str, Any]]:
    query_date = logical_date.format("YYYY-MM-DD")
    query = f"""
        SELECT STOP_NAME, stop_lat, stop_lon
        FROM (
            SELECT
                STOP_NAME,
                stop_lat,
                stop_lon,
                ROW_NUMBER() OVER (PARTITION BY STOP_NAME ORDER BY stop_lat, stop_lon) AS rn
            FROM GTFS_TEST.SCHEDULE.STOPS
            WHERE to_date(load_timestamp) = (SELECT MAX(to_date(load_timestamp)) FROM GTFS_TEST.SCHEDULE.STOPS)
        ) t
        WHERE rn = 1
    """
    results = hook.get_records(query)
    return [
        {"STOP_NAME": row[0], "stop_lat": row[1], "stop_lon": row[2]}
        for row in results
    ]

def create_traffic_intensity_table_if_not_exists(hook) -> None:
    table = "GTFS_TEST.SCHEDULE.TRAFFIC_INTENSITY"
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {table} (
        STOP_NAME STRING,
        stop_lat FLOAT,
        stop_lon FLOAT,
        current_speed FLOAT,
        free_flow_speed FLOAT,
        current_travel_time FLOAT,
        free_flow_travel_time FLOAT,
        confidence FLOAT,
        load_timestamp DATE
    )
    """
    hook.run(create_table_sql)

def insert_traffic_data_to_snowflake(traffic_data: list[dict[str, Any]], hook) -> None:
    if not traffic_data:
        return
    insert_sql = """
    INSERT INTO GTFS_TEST.SCHEDULE.TRAFFIC_INTENSITY (
        STOP_NAME, stop_lat, stop_lon, current_speed, free_flow_speed,
        current_travel_time, free_flow_travel_time, confidence, load_timestamp
    ) VALUES
    """
    values = [
        f"""(
            '{row["STOP_NAME"].replace("'", "''")}',
            {row["stop_lat"]},
            {row["stop_lon"]},
            {row["current_speed"]},
            {row["free_flow_speed"]},
            {row["current_travel_time"]},
            {row["free_flow_travel_time"]},
            {row["confidence"]},
            '{row["load_timestamp"]}'
        )"""
        for row in traffic_data
    ]
    insert_sql += ",\n".join(values)
    hook.run(insert_sql)

def process_traffic_intensity(stops: list[dict[str, Any]], logical_date: DateTime, api_keys: list[str], hook) -> None:
    """
    Processes stops in batches, fetches traffic intensity, and inserts into Snowflake.
    """
    stops = [stop for stop in stops if stop["STOP_NAME"].strip() in STOPS_INCLUDED and stop["stop_lat"] > 0 and stop["stop_lon"] > 0]
    create_traffic_intensity_table_if_not_exists(hook=hook)
    traffic_data = []
    for batch_num in range(len(stops)):
        stop = stops[batch_num]
        api_key = api_keys[batch_num % len(api_keys)]
        traffic_data.append(
            {
                "STOP_NAME": stop["STOP_NAME"],
                "stop_lat": stop["stop_lat"],
                "stop_lon": stop["stop_lon"],
                **fetch_tomtom_traffic_intensity(
                    lat=stop["stop_lat"],
                    lon=stop["stop_lon"],
                    api_key=api_key
                ),
                "load_timestamp": logical_date.format("YYYY-MM-DD")
            }
        )
    insert_traffic_data_to_snowflake(traffic_data, hook)
# %%
