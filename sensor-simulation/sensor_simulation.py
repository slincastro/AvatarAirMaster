import math
import time
import random
import json
import os
from geopy.distance import geodesic
from kafka import KafkaProducer
from threading import Thread


cities = {
    "Quito": (-0.1806532, -78.4678382),
    "Guayaquil": (-2.1709979, -79.9223592),
    "Cuenca": (-2.9001285, -79.0058965),
    "Ambato": (-1.24908, -78.61675),
    "Manta": (-0.9676533, -80.7089101),
    "Esmeraldas": (0.9636513, -79.6561136),
    "Loja": (-3.99313, -79.20422),
    "Machala": (-3.25811, -79.95589),
    "Portoviejo": (-1.05558, -80.45445),
    "Santo Domingo": (-0.25209, -79.17268),
    "Ibarra": (0.3391763, -78.1222336),
    "Quevedo": (-1.022512, -79.4604035),
    "Riobamba": (-1.6635508, -78.654646),
    "Milagro": (-2.1346495, -79.5873629),
    "Latacunga": (-0.9345704, -78.6157989),
    "La Libertad": (-2.231472, -80.9009673),
    "Babahoyo": (-1.801926, -79.5346451),
    "Otavalo": (0.2341765, -78.2661743),
    "Chone": (-0.6837542, -80.0936136),
    "El Carmen": (-0.2662036, -79.455487),
    "Azogues": (-2.739735, -78.846489),
    "Guaranda": (-1.6086139, -79.0005645),
    "Sucua": (-2.468191, -78.166504),
    "Puyo": (-1.492392, -78.0024135),
    "Nueva Loja": (0.084047, -76.8823242),
    "Samborondón": (-1.9628752, -79.7242159),
    "Macas": (-2.3095065, -78.1116327),
    "Tulcan": (0.8095047, -77.7173068),
    "Sangolquí": (-0.3126313, -78.4453967),
    "Pasaje": (-3.3343513, -79.8167435),
    "Santa Rosa": (-3.4517923, -79.9603016),
    "Rosa Zarate": (0.328231, -79.4743979),
    "Balzar": (-1.3663961, -79.9062692),
    "Huaquillas": (-3.4760833, -80.2321035),
    "Bahía de Caráquez": (-0.5937778, -80.4207213),
    "La Troncal": (-2.4194044, -79.3448473),
    "Jipijapa": (-1.333017, -80.580134),
    "Montecristi": (-1.045825, -80.657185),
    "Pelileo": (-1.32632, -78.54376),
    "Salinas": (-2.2251585, -80.958416),
    "Valencia": (-1.26794, -79.653203),
    "Zamora": (-4.069594, -78.957094),
    "Puerto Francisco de Orellana": (-0.462203, -76.993107),
    "Santa Elena": (-2.22689, -80.858732),
    "Santa Ana": (-1.222042, -80.385391),
    "Pujili": (-0.950764, -78.684171),
    "Montalvo": (-1.794189, -79.333222),
    "Pedro Carbo": (-1.819475, -80.237418),
    "San Miguel": (-1.699519, -78.97844),
    "El Triunfo": (-1.934482, -79.986672),
    "Yantzaza": (-3.82773, -78.759466),
    "Catamayo": (-3.983833, -79.354492),
    "Calceta": (-0.849184, -80.167083),
    "Cayambe": (0.050653, -78.155371),
    "Gualaceo": (-2.88566, -78.775955),
    "Naranjal": (-2.66998, -79.6213),
    "Alausi": (-2.19793, -78.846619),
    "Naranjito": (-2.215687, -79.466897),
    "Velasco Ibarra": (-1.045301, -79.638737)
}


def generate_random_coordinates_near_city(city_coords, max_distance_km=20):
    lat, lon = city_coords
    distance = random.uniform(0, max_distance_km) / 111  # Convert km to degrees (approx)
    angle = random.uniform(0, 360)
    delta_lat = distance * math.cos(math.radians(angle))
    delta_lon = distance * math.sin(math.radians(angle))
    return lat + delta_lat, lon + delta_lon

def calculate_pollution_level(sensor_type, location):
    city_distances = {city: geodesic(location, coord).km for city, coord in cities.items()}
    nearest_city = min(city_distances, key=city_distances.get)
    distance = city_distances[nearest_city]
    
    if sensor_type == "MQ-7":
        base_level = 10 if distance > 50 else 100  
    elif sensor_type == "MQ-135":
        base_level = 20 if distance > 50 else 200  

    pollution_level = base_level + random.uniform(-5, 5)
    return pollution_level, nearest_city, distance

def generate_data(topic):
    try :
        bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')

        producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
            
        )
        
        location = generate_random_coordinates_near_city(random.choice(list(cities.values())))
        mq7_level, nearest_city, mq7_distance = calculate_pollution_level("MQ-7", location)
        mq135_level, _, mq135_distance = calculate_pollution_level("MQ-135", location)
        
        
        data = {
            "location": location,
            "MQ-7": {
                "pollution_level": mq7_level
            }
            ,
            "MQ-135": {
                "pollution_level": mq135_level
            }
            , 
            "nearest_city": nearest_city,
            "distance_to_city_km": mq7_distance
        }
    
        producer.send(topic, value=data)
        print(json.dumps(data))
    except :
        print("There is some error")
    
    
    
    
if __name__ == "__main__":
    
    time.sleep(40)
   
    while True:
        topic = 'air_quality'
        try:
            generate_data(topic)
        except:
            print("no kafka available")
        
        time.sleep(2)
