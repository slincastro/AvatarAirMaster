import csv
from kafka import KafkaConsumer
import time
from kafka.errors import NoBrokersAvailable

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

csv_file = 'mensajes.csv'
with open(csv_file, mode='w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(['Mensaje']) 
    
    for message in consumer:
        print(f"Mensaje recibido: {message.value}")  
        writer.writerow([message.value])
        print(f"Mensaje guardado: {message.value}")
