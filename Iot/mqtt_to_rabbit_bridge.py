import json
import pika
import paho.mqtt.client as mqtt

MQTT_HOST = "localhost"
MQTT_PORT = 1883
MQTT_TOPIC = "devices/+/telemetry"

RABBIT_HOST = "localhost"
EXCHANGE = "iot_exchange"  # el mismo que ya venías usando

# RabbitMQ setup
rabbit_conn = pika.BlockingConnection(pika.ConnectionParameters(RABBIT_HOST))
rabbit_ch = rabbit_conn.channel()
rabbit_ch.exchange_declare(exchange=EXCHANGE, exchange_type="topic")

def mqtt_to_routing_key(mqtt_topic: str) -> str:
    # devices/<device_id>/telemetry  -> telemetry.temp (según contenido)
    # acá vamos a decidir routing_key por el payload
    return "telemetry.unknown"

def on_message(client, userdata, msg):
    try:
        data = json.loads(msg.payload.decode())
    except Exception as e:
        print("Payload inválido:", e)
        return

    # Elegimos routing_key en base a lo que venga
    # Si viene temp, mandamos telemetry.temp, si viene humidity, telemetry.humidity, etc.
    if "temp" in data:
        routing_key = "telemetry.temp"
    elif "humidity" in data:
        routing_key = "telemetry.humidity"
    else:
        routing_key = "telemetry.unknown"

    rabbit_ch.basic_publish(
        exchange=EXCHANGE,
        routing_key=routing_key,
        body=json.dumps(data)
    )
    print(f"Bridge: MQTT {msg.topic} -> RabbitMQ {routing_key}: {data}")

mqtt_client = mqtt.Client()
mqtt_client.on_message = on_message
mqtt_client.connect(MQTT_HOST, MQTT_PORT, 60)
mqtt_client.subscribe(MQTT_TOPIC)

print("Bridge corriendo. Esperando MQTT...")
mqtt_client.loop_forever()