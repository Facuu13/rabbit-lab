import pika
import json

connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

channel.exchange_declare(exchange='iot_exchange', exchange_type='topic')

# DLQ (cola para mensajes fallidos)
channel.queue_declare(queue='telemetry_db_dlq', durable=True)

# Cola principal con DLQ configurada
channel.queue_declare(
    queue='telemetry_db',
    durable=True,
    arguments={
        'x-dead-letter-exchange': '',
        'x-dead-letter-routing-key': 'telemetry_db_dlq'
    }
)

# Recibe toda telemetría
channel.queue_bind(exchange='iot_exchange', queue='telemetry_db', routing_key='telemetry.*')

def callback(ch, method, properties, body):
    try:
        data = json.loads(body.decode())

        # Simular falla
        if data.get("temp", 0) > 30:
            raise RuntimeError("DB caída simulada")

        print("Guardado OK:", data)
        ch.basic_ack(delivery_tag=method.delivery_tag)

    except Exception as e:
        print("Error procesando:", e)
        # Rechazamos y mandamos a DLQ
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

channel.basic_consume(queue='telemetry_db', on_message_callback=callback)

print("Worker DB esperando...")
channel.start_consuming()