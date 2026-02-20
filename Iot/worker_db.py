import pika
import json

connection = pika.BlockingConnection(
    pika.ConnectionParameters('localhost')
)
channel = connection.channel()

channel.exchange_declare(exchange='iot_exchange', exchange_type='topic')

result = channel.queue_declare(queue='telemetry_db', durable=True)
channel.queue_bind(
    exchange='iot_exchange',
    queue='telemetry_db',
    routing_key='telemetry.*'
)

def callback(ch, method, properties, body):
    data = json.loads(body)
    print("Guardando en DB:", data)
    ch.basic_ack(delivery_tag=method.delivery_tag)

channel.basic_consume(
    queue='telemetry_db',
    on_message_callback=callback
)

print("Worker DB esperando...")
channel.start_consuming()