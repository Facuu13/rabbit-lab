import pika

def callback(ch, method, properties, body):
    print(f"Mensaje recibido: {body.decode()}")
    ch.basic_ack(delivery_tag=method.delivery_tag)

connection = pika.BlockingConnection(
    pika.ConnectionParameters(host='localhost')
)
channel = connection.channel()

channel.queue_declare(queue='test_queue', durable=True)

channel.basic_consume(
    queue='test_queue',
    on_message_callback=callback
)

print("Esperando mensajes...")
channel.start_consuming()
