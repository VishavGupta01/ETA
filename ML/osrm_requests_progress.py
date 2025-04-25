import requests
import time
import pandas as pd
import random
from tqdm import tqdm  # Jupyter-friendly progress bar

# OSRM API URL
OSRM_URL = "https://router.project-osrm.org/route/v1/driving/{},{};{},{}?overview=false"

# File names
INPUT_FILE = "output.csv"
OUTPUT_FILE = "api_results.csv"
FAILED_REQUESTS_FILE = "failed_requests.csv"

# Load extracted data
df = pd.read_csv(INPUT_FILE)

# Ensure the dataset has a unique index
df.reset_index(drop=True, inplace=True)

# Add empty columns for API results
df["duration_sec"] = None
df["distance_m"] = None

# List to store failed requests
failed_requests = []

# Function to get route data with retries
def get_route_data(start_lat, start_lon, end_lat, end_lon, max_retries=3):
    url = OSRM_URL.format(start_lon, start_lat, end_lon, end_lat)

    for attempt in range(1, max_retries + 1):
        try:
            response = requests.get(url, timeout=10)

            if response.status_code == 200:
                data = response.json()
                routes = data.get("routes", [])
                if routes:
                    return routes[0]["duration"], routes[0]["distance"]
                else:
                    print(f"⚠️ No route found for {start_lat}, {start_lon} -> {end_lat}, {end_lon}")
                    return None, None  # No route available

            elif response.status_code == 400:
                print(f"❌ Invalid coordinates: {start_lat}, {start_lon} -> {end_lat}, {end_lon}")
                return None, None  # Invalid request, no need to retry

            elif response.status_code in [429, 500, 503]:  # Rate limit or server error
                wait_time = 2 ** (attempt - 1) + random.uniform(0.3, 1.0)  # Faster retries
                print(f"⚠️ Attempt {attempt} failed (Status: {response.status_code}). Retrying in {wait_time:.2f} sec...")
                time.sleep(wait_time)

            else:
                print(f"❌ Unexpected error: {response.status_code}. Skipping this request.")
                return None, None  # Other unexpected errors

        except Exception as e:
            print(f"⚠️ Error: {e}. Retrying...")
            time.sleep(1)  # Reduced wait time

    print(f"❌ Failed after {max_retries} attempts: {start_lat}, {start_lon} -> {end_lat}, {end_lon}")
    return None, None  # After max retries, return None

# Start processing
start_time = time.time()
total_requests = len(df)

with tqdm(total=total_requests, desc="Processing Requests", unit="req") as pbar:
    for idx, row in df.iterrows():
        start_lat, start_lon, end_lat, end_lon = row["Restaurant_latitude"], row["Restaurant_longitude"], row["Delivery_location_latitude"], row["Delivery_location_longitude"]

        # Get duration and distance
        duration, distance = get_route_data(start_lat, start_lon, end_lat, end_lon)

        # Save results
        if duration is not None and distance is not None:
            df.at[idx, "duration_sec"] = duration
            df.at[idx, "distance_m"] = distance
        else:
            failed_requests.append([row["ID"], start_lat, start_lon, end_lat, end_lon])  # Store failed request

        # Sleep to maintain rate limit (2 requests per second)
        time.sleep(random.uniform(0.4, 0.6))

        # Update progress bar
        pbar.update(1)

# Save API results including order IDs
api_results_df = df[["ID", "duration_sec", "distance_m"]]
api_results_df.to_csv(OUTPUT_FILE, index=False)
print(f"\n✅ API results saved to {OUTPUT_FILE} with order IDs.")

# Save failed requests
if failed_requests:
    pd.DataFrame(failed_requests, columns=["ID", "Restaurant_latitude", "Restaurant_longitude", "Delivery_location_latitude", "Delivery_location_longitude"]).to_csv(FAILED_REQUESTS_FILE, index=False)
    print(f"⚠️ {len(failed_requests)} requests failed. Saved to {FAILED_REQUESTS_FILE}.")
else:
    print("🎉 All requests processed successfully!")

end_time = time.time()
print(f"\n✅ Processing completed in {end_time - start_time:.2f} seconds.")