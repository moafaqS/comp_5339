import requests
import pandas as pd
import uuid
from datetime import datetime
import base64
import time
import msgpack
import paho.mqtt.client as mqtt
import os

# Class to interact with the NSW FuelCheck API
class FuelDataClient:
    def __init__(self, client_id, client_secret):
        self.client_id = client_id
        self.client_secret = client_secret
        self.token_url = "https://api.onegov.nsw.gov.au/oauth/client_credential/accesstoken"
        self.fuel_url = "https://api.onegov.nsw.gov.au/FuelPriceCheck/v1/fuel/prices"

    def get_access_token(self):
        headers = {
            "Authorization": "Basic " + base64.b64encode(f"{self.client_id}:{self.client_secret}".encode()).decode(),
            "Content-Type": "application/json"
        }
        params = {"grant_type": "client_credentials"}
        response = requests.get(self.token_url, headers=headers, params=params)
        response.raise_for_status()
        return response.json()['access_token']

    def fetch_data(self):
        token = self.get_access_token()
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
            "apikey": self.client_id,
            "transactionid": str(uuid.uuid4()),
            "requesttimestamp": datetime.utcnow().strftime("%d/%m/%Y %I:%M:%S %p"),
        }
        response = requests.get(self.fuel_url, headers=headers)
        response.raise_for_status()
        data = response.json()
        stations = pd.json_normalize(data.get("stations", []))
        prices = pd.json_normalize(data.get("prices", []))
        return stations, prices

# Clean and merge station and price data
def clean_and_merge(stations, prices):
    stations = stations.rename(columns={
        'code': 'stationcode',
        'name': 'station_name',
        'location.latitude': 'lat',
        'location.longitude': 'lon'
    }).dropna(subset=['lat', 'lon'])

    stations['lat'] = pd.to_numeric(stations['lat'], errors='coerce')
    stations['lon'] = pd.to_numeric(stations['lon'], errors='coerce')

    prices['price'] = pd.to_numeric(prices['price'], errors='coerce')
    prices = prices[prices['price'] > 0]

    latest_prices = prices.sort_values("lastupdated").groupby(['stationcode', 'fueltype']).last().reset_index()
    merged_data = pd.merge(latest_prices, stations, on='stationcode', how='left')
    return merged_data

# Dictionary to store the last published price for each (station, fueltype)
last_sent_prices = {}

# Publish only new or changed records to the MQTT broker
def publish_if_changed(client, fuel_data):
    global last_sent_prices
    updated_count = 0

    for _, row in fuel_data.iterrows():
        record = row.dropna().to_dict()
        key = f"{record.get('stationcode')}_{record.get('fueltype')}"
        current_price = record.get('price')

        if last_sent_prices.get(key) != current_price:
            payload = msgpack.packb(record)
            client.publish("fuel/new_prices", payload, qos=0)
            last_sent_prices[key] = current_price
            updated_count += 1
            print(f"Published: {key} = {current_price}¢")

        time.sleep(0.1)

    print(f"Published {updated_count} new or changed records")

# Main loop to continuously fetch, process, and publish fuel data
def main():
    client_id = 'CvRQC1qC8akmwp9Qfy5owgzWk8izoa9Q'
    client_secret = 'SNmuC7nzn3IISVcG'

    mqtt_client = mqtt.Client()
    mqtt_client.connect("broker.hivemq.com", 1883, 60)
    mqtt_client.loop_start()

    fuel_client = FuelDataClient(client_id, client_secret)

    while True:
        try:
            print("Fetching latest fuel data...")
            stations, prices = fuel_client.fetch_data()
            combined_data = clean_and_merge(stations, prices)

            combined_data.to_csv("fuel_prices_latest.csv", index=False)
            print(f"Saved {len(combined_data)} records to 'fuel_prices_latest.csv'")

            publish_if_changed(mqtt_client, combined_data)

            print("Waiting 60 seconds for next update...\n")
        except Exception as e:
            print(f"An error occurred: {e}")

        time.sleep(60)

if __name__ == "__main__":
    main()