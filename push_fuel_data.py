import requests
import pandas as pd
import uuid
from datetime import datetime
import base64
import time
import msgpack
import paho.mqtt.client as mqtt
import os

class FuelDataClient:
    def __init__(self, client_id, client_secret):
        self.client_id = client_id
        self.client_secret = client_secret
        self.token_url = "https://api.onegov.nsw.gov.au/oauth/client_credential/accesstoken"
        self.fuel_url = "https://api.onegov.nsw.gov.au/FuelPriceCheck/v1/fuel/prices"

    # Request an OAuth2 access token using client credentials
    def get_access_token(self):
        headers = {
            "Authorization": "Basic " + base64.b64encode(f"{self.client_id}:{self.client_secret}".encode()).decode(),
            "Content-Type": "application/json"
        }
        params = {"grant_type": "client_credentials"}
        r = requests.get(self.token_url, headers=headers, params=params)
        r.raise_for_status()
        return r.json()['access_token']

    # Fetch real-time fuel prices and station data
    def fetch_data(self):
        token = self.get_access_token()
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
            "apikey": self.client_id,
            "transactionid": str(uuid.uuid4()),
            "requesttimestamp": datetime.utcnow().strftime("%d/%m/%Y %I:%M:%S %p"),
        }
        r = requests.get(self.fuel_url, headers=headers)
        r.raise_for_status()
        data = r.json()
        stations = pd.json_normalize(data.get("stations", []))
        prices = pd.json_normalize(data.get("prices", []))
        return stations, prices

# Data Cleaning 
def clean_and_merge(stations, prices):
    # Rename and clean station coordinates
    stations = stations.rename(columns={
        'code': 'stationcode',
        'name': 'station_name',
        'location.latitude': 'lat',
        'location.longitude': 'lon'
    }).dropna(subset=['lat', 'lon'])

    stations['lat'] = pd.to_numeric(stations['lat'], errors='coerce')
    stations['lon'] = pd.to_numeric(stations['lon'], errors='coerce')

    # Filter and clean fuel prices
    prices['price'] = pd.to_numeric(prices['price'], errors='coerce')
    prices = prices[prices['price'] > 0]

    # Keep only the latest price per station and fuel type
    latest = prices.sort_values("lastupdated").groupby(['stationcode', 'fueltype']).last().reset_index()

    # Merge price info with station metadata
    merged = pd.merge(latest, stations, on='stationcode', how='left')
    return merged

def publish_records(client, records):
    for _, row in records.iterrows():
        # Convert each row to a dictionary then encode with MessagePack
        record = row.dropna().to_dict()
        payload = msgpack.packb(record)

        # Publish to the fuel/new_prices topic
        client.publish("fuel/new_prices", payload, qos=0)
        # print(f"Published: {record.get('stationcode')} - {record.get('fueltype')}")

        time.sleep(0.1)

def main():
    # Your NSW API client credentials
    client_id = 'CvRQC1qC8akmwp9Qfy5owgzWk8izoa9Q'
    client_secret = 'SNmuC7nzn3IISVcG'

    # Connect with a broker
    mqtt_client = mqtt.Client()
    mqtt_client.connect("broker.hivemq.com", 1883, 60)
    mqtt_client.loop_start()

    fuel_client = FuelDataClient(client_id, client_secret)

    # Repeat fetch + publish every 60 seconds
    while True:
        try:
            print("Fetching new fuel data...")
            stations, prices = fuel_client.fetch_data()
            merged_data = clean_and_merge(stations, prices)

            # Save to CSV 
            csv_path = "fuel_prices_latest.csv"
            merged_data.to_csv(csv_path, index=False)
            print(f"Saved {len(merged_data)} records to {csv_path}")

            # Publish records 
            publish_records(mqtt_client, merged_data)

            print("Publishing complete. Waiting 60 seconds before next update.")
        except Exception as e:
            print(f"Error: {e}")

        time.sleep(60)

if __name__ == "__main__":
    main()