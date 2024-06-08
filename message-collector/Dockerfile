# Usa una imagen base de Python
FROM python:3.9-slim

# Establece el directorio de trabajo
WORKDIR /app

# Copia los archivos necesarios
COPY kafka_consumer.py .

# Instala las dependencias
RUN pip install kafka-python

# Comando para ejecutar el script de Python
CMD [ "python", "kafka_consumer.py"]
