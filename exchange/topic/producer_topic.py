import pika

connection = pika.BlockingConnection(
    pika.ConnectionParameters('localhost')
)
channel = connection.channel()

channel.exchange_declare(exchange='topic_logs', exchange_type='topic')

messages = [
    ("sensor.temp", "Temperatura 25C"),
    ("sensor.humidity", "Humedad 60%"),
    ("system.error", "Error crítico")
]

for routing_key, message in messages:
    channel.basic_publish(
        exchange='topic_logs',
        routing_key=routing_key,
        body=message
    )
    print(f"Enviado [{routing_key}] {message}")

connection.close()
