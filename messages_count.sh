#!/bin/bash

TOPIC=$1

if [ -z "$TOPIC" ]; then
  echo "Uso: $0 <nombre-del-topico>"
  exit 1
fi

# Ejecutar el comando para obtener los offsets
OUTPUT=$(docker exec kafka kafka-run-class kafka.tools.GetOffsetShell --broker-list localhost:9092 --topic $TOPIC --time -1 | tr -d '\r')

# Inicializar el contador de mensajes
TOTAL_MESSAGES=0

# Iterar sobre cada línea del resultado
while IFS= read -r LINE; do
  OFFSET=$(echo "$LINE" | awk -F ":" '{print $3}')
  TOTAL_MESSAGES=$((TOTAL_MESSAGES + OFFSET + 1))
done <<< "$OUTPUT"

# Mostrar el resultado final
echo "El número total de mensajes en el tópico '$TOPIC' es: $TOTAL_MESSAGES"
