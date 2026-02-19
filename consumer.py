import pika

def callback(ch, method, properties, body):
    print(f"Mensaje recibido: {body.decode()}")
    # ❌ NO hacemos basic_ack()

connection = pika.BlockingConnection(
    pika.ConnectionParameters(host='localhost')
)
channel = connection.channel()

channel.queue_declare(queue='test_queue', durable=True)

channel.basic_consume(
    queue='test_queue',
    on_message_callback=callback,
    auto_ack=False
)

print("Esperando mensajes...")
channel.start_consuming()
