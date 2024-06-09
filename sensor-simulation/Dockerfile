FROM python:3.8-slim

WORKDIR /app

COPY requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY sensor_simulation.py sensor_simulation.py

CMD ["python", "sensor_simulation.py"]
