FROM python:3.9-slim

WORKDIR /app

COPY kafka_consumer.py .

RUN pip install kafka-python

CMD [ "python", "kafka_consumer.py"]
