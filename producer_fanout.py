import pika

connection = pika.BlockingConnection(
    pika.ConnectionParameters('localhost')
)
channel = connection.channel()

# Crear exchange tipo fanout
channel.exchange_declare(exchange='logs', exchange_type='fanout')

message = "Evento importante!"
channel.basic_publish(
    exchange='logs',
    routing_key='',
    body=message
)

print("Mensaje broadcast enviado")

connection.close()
