#!/usr/bin/env python3
"""
map_krakow_districts_validation.py

Loads district boundaries from 'districts.geojson',
computes centroids for each district, and renders
an interactive map with Folium showing both:
  - district borders
  - centroid markers labeled by district name
"""

import json
from pathlib import Path

from shapely.geometry import shape
import folium

# Paths
BASE_DIR       = Path(__file__).parent
GEOJSON_PATH   = BASE_DIR / "districts.geojson"
OUTPUT_MAP     = BASE_DIR / "krakow_districts_map.html"

# Load GeoJSON features
with open(GEOJSON_PATH, "r", encoding="utf-8") as f:
    geojson_data = json.load(f)

# Compute centroids
centroids = []
for feature in geojson_data["features"]:
    props    = feature["properties"]
    geom     = feature["geometry"]
    polygon  = shape(geom)
    centroid = polygon.centroid
    centroids.append({
        "name":      props.get("name", "UNKNOWN"),
        "latitude":  round(centroid.y, 6),
        "longitude": round(centroid.x, 6)
    })

# Initialize map centered on Kraków
m = folium.Map(location=[50.06465, 19.94498], zoom_start=12)

# Add district borders as a GeoJSON layer
folium.GeoJson(
    geojson_data,
    name="District Borders",
    style_function=lambda feature: {
        "color": "#3333ff",
        "weight": 2,
        "fillOpacity": 0.1
    }
).add_to(m)

# Add centroid markers
for c in centroids:
    folium.CircleMarker(
        location=[c["latitude"], c["longitude"]],
        radius=5,
        color="red",
        fill=True,
        fill_color="red",
        fill_opacity=0.7,
        popup=c["name"]
    ).add_to(m)

# Add layer control and save
folium.LayerControl().add_to(m)
m.save(str(OUTPUT_MAP))

print(f"Map with {len(centroids)} centroids saved to {OUTPUT_MAP}")
