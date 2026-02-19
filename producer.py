import pika

# Conexión a RabbitMQ
connection = pika.BlockingConnection(
    pika.ConnectionParameters(host='localhost')
)
channel = connection.channel()

# Asegurar que la queue exista
channel.queue_declare(queue='test_queue', durable=True)

# Enviar mensaje
message = "Hola Facu desde RabbitMQ"
channel.basic_publish(
    exchange='',
    routing_key='test_queue',
    body=message
)

print(f"Mensaje enviado: {message}")

connection.close()
