import requests
from typing import Any
import time
from math import ceil
import datetime
from pendulum import DateTime

BATCH_SIZE = 100
STOPS_INCLUDED = {
    'AGH / UR': (50.06266, 19.92319),
    'Aleja Róż': (50.07479, 20.03887),
    'Arka': (50.08349, 20.03039),
    'Armii Krajowej': (50.0717, 19.8909),
    'Azory': (50.08679, 19.90879),
    'Banacha': (50.10132, 19.96315),
    'Beskidzka': (50.01837, 19.95159),
    'Bieżanowska': (50.0228, 19.98207),
    'Biprostal': (50.07244, 19.91516),
    'Biskupa Prandoty': (50.07802, 19.94865),
    'Bonarka': (50.02877, 19.95401),
    'Bratysławska': (50.08273, 19.93465),
    'Brodowicza': (50.06908, 19.96111),
    'Bronowice SKA': (50.08082, 19.89066),
    'Bujaka': (50.00958, 19.94843),
    'Chełm': (50.06754, 19.84662),
    'Chopina': (50.06753, 19.91725),
    'Cmentarz Rakowicki Zachód': (50.07564, 19.94681),
    'Conrada': (50.08857, 19.89829),
    'Cracovia Błonia': (50.05878, 19.92169),
    'Czarnowiejska': (50.06621, 19.92292),
    'Dobrego Pasterza': (50.08833, 19.95676),
    'Dunikowskiego': (50.09062, 20.01673),
    'Dworzec Główny Wschód': (50.06848, 19.94943),
    'Dworzec Główny Zachód': (50.0672, 19.9445),
    'Fatimska': (50.09316, 20.02911),
    'Godlewskiego': (50.08232, 19.85341),
    'Góra Borkowska': (50.00664, 19.92454),
    'Halszki': (50.00551, 19.94895),
    'Imbramowska': (50.08753, 19.9483),
    'Jana Kazimierza': (50.09492, 20.04706),
    'Jerzmanowskiego': (50.00971, 20.00924),
    'Jubilat': (50.05492, 19.92692),
    'Judyma': (50.00145, 19.92179),
    'Kamieńskiego': (50.02888, 19.9569),
    'Kamieńskiego Wiadukt': (50.03111, 19.94914),
    'Kapelanka': (50.04321, 19.92265),
    'Kawiory': (50.06833, 19.91295),
    'Kliny Zacisze': (50.00124, 19.90027),
    'Kobierzyńska': (50.03406, 19.92587),
    'Konopnickiej': (50.05274, 19.92958),
    'Krowoderskich Zuchów': (50.08863, 19.92545),
    'Ks. Meiera': (50.09741, 19.96191),
    'Kurzei': (50.09515, 19.98986),
    'Kąpielowa': (49.99706, 19.91956),
    'Leszka Białego': (50.09581, 20.05175),
    'Lindego': (50.08004, 19.87063),
    'Lubicz': (50.06499, 19.951),
    'Ludwinów': (50.04324, 19.93474),
    'Mackiewicza': (50.08959, 19.94397),
    'Majora': (50.08797, 19.96422),
    'Makowa': (50.02707, 19.96661),
    'Makowskiego': (50.08757, 19.91728),
    'Malborska': (50.02604, 19.97483),
    'Marchołta': (50.09264, 19.98771),
    'Mazowiecka': (50.07783, 19.91771),
    'Miasteczko Studenckie AGH': (50.06962, 19.90645),
    'Miechowity': (50.08305, 19.97092),
    'Mistrzejowice': (50.09486, 19.995525),
    'Miśnieńska': (50.0944, 20.00052),
    'Muzeum Narodowe': (50.05864, 19.9252),
    'Młynówka SKA': (50.08119, 19.85907),
    'Narzymskiego': (50.07369, 19.96631),
    'Nowosądecka': (50.013437, 19.964684),
    'Nowy Kleparz': (50.0731, 19.93482),
    'Nowy Prokocim': (50.01591, 20.01175),
    'Olsza II': (50.08484, 19.96885),
    'Opolska Estakada': (50.08483, 19.95346),
    'Os. Jagiellońskie': (50.08778, 20.02781),
    'Os. Kurdwanów': (50.00441, 19.96035),
    'Os. Mistrzejowice Nowe': (50.09491, 20.02132),
    'Os. Oświecenia': (50.08881, 19.98859),
    'Os. Pod Fortem': (49.99862, 19.89723),
    'Os. Podwawelskie': (50.04607, 19.93265),
    'Os. Zgody': (50.07644, 20.03128),
    'Os. Złotego Wieku': (50.093667, 20.004478),
    'Park Wodny': (50.09005, 19.98204),
    'Piaski Wielkie': (50.00583, 19.98294),
    'Piastowska': (50.05754, 19.90018),
    'Pilotów': (50.07791, 19.97026),
    'Plac Inwalidów': (50.06864, 19.92707),
    'Podgórze SKA': (50.04181, 19.96148),
    'Politechnika': (50.070807, 19.945062),
    'Powstańców Garaże': (50.09497, 19.96752),
    'Prokocim Szpital': (50.01422, 20.00012),
    'Przybyszewskiego': (50.07063, 19.89899),
    'Pszenna': (50.0202, 19.95645),
    'Radio Kraków': (50.07175, 19.92933),
    'Rondo Barei': (50.08814, 19.97289),
    'Rondo Czyżyńskie': (50.07246, 20.01664),
    'Rondo Grunwaldzkie': (50.04825, 19.93234),
    'Rondo Hipokratesa': (50.090025, 20.021395),
    'Rondo Kocmyrzowskie im. Ks. Gorzelanego': (50.0788, 20.0281),
    'Rondo Matecznego': (50.03588, 19.94138),
    'Rondo Mogilskie': (50.06476, 19.95902),
    'Rondo Młyńskie': (50.07975, 19.97228),
    'Rondo Ofiar Katynia': (50.08663, 19.89106),
    'Rondo Piastowskie': (50.09416, 20.0111),
    'Różyckiego': (50.08792, 19.91392),
    'Stachiewicza': (50.08489, 19.91634),
    'Stoczniowców': (50.04852, 19.98205),
    'Stojałowskiego': (50.00504, 19.95605),
    'Struga': (50.07441, 20.04715),
    'Surzyckiego': (50.0389, 20.00416),
    'Szpital Jana Pawła II': (50.08837, 19.93856),
    'Szpital Narutowicza': (50.080625, 19.937974),
    'Szwedzka': (50.0467, 19.92542),
    'Sławka': (50.0257, 19.96004),
    'Słomiana': (50.04072, 19.9245),
    'Teatr Ludowy': (50.08103, 20.03406),
    'Teatr Słowackiego': (50.06401, 19.94509),
    'Teligi': (50.016511, 20.007916),
    'UR Balicka': (50.08084, 19.86548),
    'UR al. 29 Listopada': (50.08212, 19.95156),
    'Wańkowicza': (50.08656, 20.04732),
    'Witkowicka': (50.10855, 19.96541),
    'Wiślicka': (50.08718, 20.00126),
    'Wlotowa': (50.01887, 19.98811),
    'Wola Duchacka': (50.01971, 19.96056),
    'Wyki': (50.09271, 19.92233),
    'Węgrzce A1': (50.11931, 19.96695),
    'Węgrzce Starostwo Powiatowe': (50.11409, 19.96643),
    'Węzeł Wielicki': (50.00576, 20.01821),
    'Władysława Jagiełły': (50.09497, 20.04007),
    'Zajezdnia Wola Duchacka': (50.02333, 19.96028),
    'Zarzecze': (50.07675, 19.88864),
    'Ćwiklińskiej': (50.01556, 20.02035),
    'Łagiewniki': (50.02881, 19.93622),
    'Łempickiego': (50.0783, 20.06001),
    'Łobzów SKA': (50.08152, 19.91612)
}


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
    create_traffic_intensity_table_if_not_exists(hook=hook)
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

def process_traffic_intensity(logical_date: DateTime, api_keys: list[str]) -> list:
    """
    Processes stops in batches, fetches traffic intensity, and inserts into Snowflake.
    """
    stops = list(STOPS_INCLUDED.keys())
    traffic_data = []
    for batch_num in range(len(stops)):
        stop = stops[batch_num]
        api_key = api_keys[batch_num % len(api_keys)]
        traffic_data.append(
            {
                "STOP_NAME": stop,
                "stop_lat": STOPS_INCLUDED[stop][0],
                "stop_lon": STOPS_INCLUDED[stop][1],
                **fetch_tomtom_traffic_intensity(
                    lat=STOPS_INCLUDED[stop][0],
                    lon=STOPS_INCLUDED[stop][1],
                    api_key=api_key
                ),
                "load_timestamp": logical_date.format("YYYY-MM-DD")
            }
        )
    return traffic_data


def save_traffic_data_to_disk(traffic_data: list[dict[str, Any]], logical_date: DateTime, output_dir: str) -> str:
    import json
    import os
    from pathlib import Path
    
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    file_path = os.path.join(output_dir, f"traffic_{logical_date.format('YYYYMMDD_HHmm')}.json")
    with open(file_path, 'w') as f:
        json.dump(traffic_data, f)
    return file_path


def load_traffic_data_from_disk(data_dir: str) -> list[dict[str, Any]]:
    import json
    import os
    from pathlib import Path
    
    all_data: list[dict[str, Any]] = []
    data_path = Path(data_dir)
    
    if not data_path.exists():
        return all_data
    
    for file_path in sorted(data_path.glob("traffic_*.json")):
        try:
            with open(file_path, 'r') as f:
                batch = json.load(f)
                if batch:
                    all_data.extend(batch)
        except Exception as e:
            print(f"Error loading {file_path}: {e}")
    
    return all_data


def purge_traffic_data_from_disk(data_dir: str) -> None:
    import os
    from pathlib import Path
    
    data_path = Path(data_dir)
    if data_path.exists():
        for file_path in data_path.glob("traffic_*.json"):
            try:
                os.remove(file_path)
            except Exception as e:
                print(f"Error deleting {file_path}: {e}")
# %%
