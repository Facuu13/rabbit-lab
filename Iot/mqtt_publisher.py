import json
import time
import paho.mqtt.client as mqtt

MQTT_HOST = "localhost"
MQTT_PORT = 1883
TOPIC = "devices/esp32-01/telemetry"

client = mqtt.Client()
client.connect(MQTT_HOST, MQTT_PORT, 60)

payload = {
    "device_id": "esp32-01",
    "temp": 35,
    "humidity": 60,
}

client.publish(TOPIC, json.dumps(payload))
print("MQTT publicado:", TOPIC, payload)

client.disconnect()