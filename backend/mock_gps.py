from mapbox import Directions
from dotenv import load_dotenv
import os

# Load token from .env
load_dotenv()
MAPBOX_TOKEN = os.getenv("MAPBOX_ACCESS_TOKEN")

# Initialize Directions service
directions_service = Directions(access_token=MAPBOX_TOKEN)

# Example: From Delhi (India Gate) to Connaught Place
origin = (77.2295, 28.6129)  # (longitude, latitude)
destination = (77.2195, 28.6315)

# Request route
response = directions_service.directions(
    [origin, destination],
    profile='mapbox/driving',
    steps=True,
    geometries='geojson'  # gets route as a GeoJSON LineString
)

# Check response
if response.status_code == 200:
    data = response.json()
    route = data['routes'][0]['geometry']['coordinates']
    print("Fetched route with", len(route), "points.")
else:
    print("Error:", response.status_code, response.text)

print(route)