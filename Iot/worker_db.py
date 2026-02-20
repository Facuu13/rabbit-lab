import json
import pika

MAX_RETRIES = 3

connection = pika.BlockingConnection(pika.ConnectionParameters("localhost"))
channel = connection.channel()

channel.exchange_declare(exchange="iot_exchange", exchange_type="topic")

# DLQ
channel.queue_declare(queue="telemetry_db_dlq", durable=True)

# Cola principal con DLQ configurada
channel.queue_declare(
    queue="telemetry_db",
    durable=True,
    arguments={
        "x-dead-letter-exchange": "",
        "x-dead-letter-routing-key": "telemetry_db_dlq",
    },
)

channel.queue_bind(exchange="iot_exchange", queue="telemetry_db", routing_key="telemetry.*")

def get_retries(properties: pika.BasicProperties) -> int:
    headers = (properties.headers or {})
    try:
        return int(headers.get("x-retries", 0))
    except Exception:
        return 0

def republish_with_retry(body: bytes, properties: pika.BasicProperties, retries: int):
    headers = dict((properties.headers or {}))
    headers["x-retries"] = retries

    new_props = pika.BasicProperties(
        headers=headers,
        delivery_mode=2,  # persistente
        content_type=properties.content_type or "application/json",
    )

    # Re-publicamos al mismo exchange/routing_key para que vuelva a caer en telemetry_db
    channel.basic_publish(
        exchange="iot_exchange",
        routing_key="telemetry.temp",  # simplificado: si querés, luego lo calculamos según payload
        body=body,
        properties=new_props,
    )

def callback(ch, method, properties, body):
    retries = get_retries(properties)

    try:
        data = json.loads(body.decode())

        # Simular falla
        if data.get("temp", 0) > 30:
            raise RuntimeError("DB caída simulada")

        print("✅ Guardado OK:", data, "| retries:", retries)
        ch.basic_ack(delivery_tag=method.delivery_tag)

    except Exception as e:
        if retries < MAX_RETRIES:
            next_retry = retries + 1
            print(f"⚠️ Error: {e} | reintento {next_retry}/{MAX_RETRIES}")

            # Re-publico con contador incrementado
            republish_with_retry(body, properties, next_retry)

            # Confirmo el original para no duplicar infinito
            ch.basic_ack(delivery_tag=method.delivery_tag)
        else:
            print(f"❌ Error: {e} | superó {MAX_RETRIES} → DLQ")
            # No requeue, entonces va a DLQ por configuración
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

channel.basic_consume(queue="telemetry_db", on_message_callback=callback)

print("Worker DB con retry esperando...")
channel.start_consuming()