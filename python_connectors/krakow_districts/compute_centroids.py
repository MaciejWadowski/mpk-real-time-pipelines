#!/usr/bin/env python3
"""
compute_centroids.py

Reads district boundaries from districts.geojson,
calculates the centroid of each district polygon,
and writes the result to a JSON file for use in the weather data pipeline.
"""

import json
from shapely.geometry import shape
from pathlib import Path

# File paths
GEOJSON_PATH = Path(__file__).parent / "districts.geojson"
OUTPUT_JSON  = Path(__file__).parent / "input_values.json"

# Date range parameters
START_DATE = "2025-05-21"
END_DATE   = "2025-07-21"

# Load the GeoJSON file containing district geometries
with open(GEOJSON_PATH, "r", encoding="utf-8") as f:
    geojson_data = json.load(f)

# Prepare the output structure
output = {
    "start_date": START_DATE,
    "end_date":   END_DATE,
    "locations": []
}

# Iterate over each feature (district) in the GeoJSON
for feature in geojson_data["features"]:
    properties = feature["properties"]
    geometry = feature["geometry"]
    polygon = shape(geometry)

    # Calculate the centroid of the polygon geometry
    centroid = polygon.centroid

    # Append district name and centroid coordinates to the list
    output["locations"].append({
        "district":  properties.get("name", "UNKNOWN"),
        "latitude":  round(centroid.y, 6),
        "longitude": round(centroid.x, 6)
    })

# Write the resulting list of centroids to a JSON file (UTF-8)
with open(OUTPUT_JSON, "w", encoding="utf-8") as f:
    json.dump(output, f, ensure_ascii=False, indent=2)

print(f"Wrote {len(output['locations'])} locations to {OUTPUT_JSON}")
