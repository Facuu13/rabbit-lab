import pika
import json

connection = pika.BlockingConnection(
    pika.ConnectionParameters('localhost')
)
channel = connection.channel()

channel.exchange_declare(exchange='iot_exchange', exchange_type='topic')

data = {
    "device_id": "esp32-01",
    "temp": 35,
    "humidity": 60
}

channel.basic_publish(
    exchange='iot_exchange',
    routing_key='telemetry.temp',
    body=json.dumps(data)
)

print("Telemetría enviada")

connection.close()