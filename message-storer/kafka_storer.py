import time
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable
from pymongo import MongoClient

def dibujar_colector():
    colector = [
        "        _____  ",
        "       /     \\ ",
        "      /       \\",
        "     |  (o) (o)|",
        "     |    _    |",
        "      \\  \\_/  /",
        "       \\_____/ ",
        "        | | |  ",
        "        | | |  ",
        "        | | |  ",
        "       /  |  \\ ",
        "      /   |   \\",
        "     /    |    \\"
    ]
    
    for linea in colector:
        print(linea)

dibujar_colector()

print("Esperando 90 segundos...")
time.sleep(90)

connected = False
while not connected:
    try:
        consumer = KafkaConsumer(
            'air_quality',
            bootstrap_servers=['kafka:9092'],
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            group_id='mi_grupo',
            value_deserializer=lambda x: x.decode('utf-8')
        )
        connected = True
        print("Conexión a Kafka establecida.")
    except NoBrokersAvailable:
        print("Kafka no está disponible. Reintentando en 5 segundos...")
        time.sleep(5)

mongo_client = MongoClient('mongo', 27017)
db = mongo_client.air_quality
collection = db.readings

for message in consumer:
    print(f"Mensaje recibido: {message.value}")
    data = {
        "mensaje": message.value,
        "timestamp": time.time()
    }
    collection.insert_one(data)
    print(f"Mensaje guardado en MongoDB: {data}")
