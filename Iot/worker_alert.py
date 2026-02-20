import pika
import json

connection = pika.BlockingConnection(
    pika.ConnectionParameters('localhost')
)
channel = connection.channel()

channel.exchange_declare(exchange='iot_exchange', exchange_type='topic')

result = channel.queue_declare(queue='telemetry_alert', durable=True)
channel.queue_bind(
    exchange='iot_exchange',
    queue='telemetry_alert',
    routing_key='telemetry.temp'
)

def callback(ch, method, properties, body):
    data = json.loads(body)

    if data["temp"] > 30:
        print("⚠️ ALERTA! Temperatura alta:", data["temp"])
    else:
        print("Temperatura normal")

    ch.basic_ack(delivery_tag=method.delivery_tag)

channel.basic_consume(
    queue='telemetry_alert',
    on_message_callback=callback
)

print("Worker Alert esperando...")
channel.start_consuming()