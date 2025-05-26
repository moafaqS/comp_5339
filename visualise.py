import streamlit as st
import folium
from streamlit_folium import st_folium
import paho.mqtt.client as mqtt
import msgpack
import threading
import time
from datetime import datetime
import queue

st.set_page_config(page_title="NSW FuelCheck Dashboard", layout="wide")

# Initialize session state
if 'stations_data' not in st.session_state:
    st.session_state.stations_data = {}
if 'fuel_types' not in st.session_state:
    st.session_state.fuel_types = set(['E10', 'Unleaded 91', 'Unleaded 95', 'Unleaded 98', 'Diesel', 'Premium Diesel'])
if 'message_queue' not in st.session_state:
    st.session_state.message_queue = queue.Queue()
if 'center' not in st.session_state:
    st.session_state.center = [-33.8688, 151.2093]
if 'zoom' not in st.session_state:
    st.session_state.zoom = 10
if 'markers' not in st.session_state:
    st.session_state.markers = []

class MessageHandler:
    def __init__(self, msg_queue):
        self.msg_queue = msg_queue
        self.client = mqtt.Client()
        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message
        
    def on_connect(self, client, userdata, flags, rc):
        if rc == 0:
            client.subscribe("fuel/new_prices")
        
    def on_message(self, client, userdata, msg):
        try:
            data = msgpack.unpackb(msg.payload, raw=False)
            self.msg_queue.put(data)
        except Exception as e:
            pass
    
    def connect(self):
        try:
            self.client.connect("broker.hivemq.com", 1883, 60)
            self.client.loop_start()
        except Exception as e:
            pass

def handle_messages():
    while not st.session_state.message_queue.empty():
        try:
            data = st.session_state.message_queue.get_nowait()
            
            station_code = data.get('stationcode')
            fuel_type = data.get('fueltype')
            
            if station_code and fuel_type:
                st.session_state.fuel_types.add(fuel_type)
                
                if station_code not in st.session_state.stations_data:
                    st.session_state.stations_data[station_code] = {
                        'station_name': data.get('station_name', 'Unknown'),
                        'brand': data.get('brand', 'Unknown'),
                        'address': data.get('address', 'Unknown'),
                        'lat': float(data.get('lat', 0)),
                        'lon': float(data.get('lon', 0)),
                        'prices': {}
                    }
                
                st.session_state.stations_data[station_code]['prices'][fuel_type] = {
                    'price': float(data.get('price', 0)),
                    'lastupdated': data.get('lastupdated', '')
                }
                
        except queue.Empty:
            break
        except Exception as e:
            pass

def build_popup(station_data, station_code):
    """Create popup content for a station marker"""
    price_rows = ""
    for fuel_type, price_info in station_data['prices'].items():
        price_rows += f"<tr><td><b>{fuel_type}</b></td><td>${price_info['price']:.2f}</td><td>{price_info['lastupdated']}</td></tr>"
    
    html_content = f"""
    <div style="width: 280px; font-family: Arial, sans-serif;">
        <h4 style="margin: 0 0 10px 0; color: #333;">{station_data['station_name']}</h4>
        <p style="margin: 5px 0;"><b>Brand:</b> {station_data['brand']}</p>
        <p style="margin: 5px 0;"><b>Address:</b> {station_data['address']}</p>
        <hr style="margin: 10px 0;">
        <h5 style="margin: 5px 0; color: #333;">Fuel Prices:</h5>
        <table style="width: 100%; border-collapse: collapse; font-size: 12px;">
            <thead>
                <tr style="background-color: #f0f0f0;">
                    <th style="padding: 4px; border: 1px solid #ddd; text-align: left;">Type</th>
                    <th style="padding: 4px; border: 1px solid #ddd; text-align: left;">Price</th>
                    <th style="padding: 4px; border: 1px solid #ddd; text-align: left;">Updated</th>
                </tr>
            </thead>
            <tbody>
                {price_rows}
            </tbody>
        </table>
    </div>
    """
    return html_content

# Initialize message handler
if 'msg_handler' not in st.session_state:
    st.session_state.msg_handler = MessageHandler(st.session_state.message_queue)
    st.session_state.msg_handler.connect()

# Main dashboard
st.title("NSW FuelCheck Dashboard")

# Process messages
handle_messages()

# Fuel type selector
fuel_list = ["All"] + sorted(list(st.session_state.fuel_types))
selected_fuel = st.selectbox("Select default fuel type:", fuel_list)

# Create map
m = folium.Map(location=st.session_state.center, zoom_start=st.session_state.zoom)
feature_group = folium.FeatureGroup(name="Markers")

# Build markers
st.session_state.markers = []

for station_code, station_data in st.session_state.stations_data.items():
    if station_data['lat'] and station_data['lon']:
        # Check if station should be displayed
        show_station = False
        
        if selected_fuel == "All":
            show_station = bool(station_data['prices'])
        else:
            show_station = selected_fuel in station_data['prices']
        
        if show_station:
            # Create popup content
            popup_content = build_popup(station_data, station_code)
            
            # Create marker
            marker = folium.Marker(
                location=[station_data['lat'], station_data['lon']],
                popup=folium.Popup(popup_content, max_width=320)
            )
            st.session_state.markers.append(marker)

# Add markers to group
for marker in st.session_state.markers:
    feature_group.add_child(marker)

# Display map
dashboard_map = st_folium(
    m,
    center=st.session_state.center,
    zoom=st.session_state.zoom,
    key="fuel_map",
    feature_group_to_add=feature_group,
    height=600,
    width="100%"
)

# Auto-refresh every 15 seconds
time.sleep(15)
st.rerun()