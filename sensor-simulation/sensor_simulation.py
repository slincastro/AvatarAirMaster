import time
import random
import json
import os
from geopy.distance import geodesic
from kafka import KafkaProducer



# Coordenadas aproximadas de algunas ciudades en Ecuador
cities = {
    "Quito": (-0.180653, -78.467834),
    "Guayaquil": (-2.170998, -79.922359),
    "Cuenca": (-2.897400, -79.004531)
}

def generate_random_coordinates():
    lat = random.uniform(-5.0, 1.5)  # Coordenadas aproximadas de Ecuador
    lon = random.uniform(-81.0, -75.0)
    return lat, lon

def calculate_pollution_level(sensor_type, location):
    city_distances = {city: geodesic(location, coord).km for city, coord in cities.items()}
    nearest_city = min(city_distances, key=city_distances.get)
    distance = city_distances[nearest_city]
    
    if sensor_type == "MQ-7":
        base_level = 10 if distance > 50 else 100  # Nivel base de CO
    elif sensor_type == "MQ-135":
        base_level = 20 if distance > 50 else 200  # Nivel base de contaminación de aire

    # Añadir variación aleatoria
    pollution_level = base_level + random.uniform(-5, 5)
    return pollution_level, nearest_city, distance

if __name__ == "__main__":
    
    time.sleep(30)
    location = generate_random_coordinates()
    bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')

    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
     
    while True:
       

        mq7_level, nearest_city, mq7_distance = calculate_pollution_level("MQ-7", location)
        mq135_level, _, mq135_distance = calculate_pollution_level("MQ-135", location)
        
        data = {
            "location": location,
            "MQ-7": {
                "pollution_level": mq7_level,
                "nearest_city": nearest_city,
                "distance_to_city_km": mq7_distance
            },
            "MQ-135": {
                "pollution_level": mq135_level,
                "nearest_city": nearest_city,
                "distance_to_city_km": mq135_distance
            }
        }
        
        producer.send('air_quality', value=data)
        
        print(json.dumps(data))
        time.sleep(2)
