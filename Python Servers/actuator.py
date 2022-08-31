import paho.mqtt.client as mqtt
import json
import requests

BROKER = "server.matmacsystem.it"
PORT = 1883
TOPIC = "mtds/sensor/data/A/0/S/3"

HUM_LIM = 60
TEMP_LIM = 30

global hum_act_state
hum_act_state = 0

global temp_act_state
temp_act_state = 0


def send_telegram_notification(text):
    requests.get('https://api.telegram.org/bot5456641962:AAEzqKwRheRBsig65-4fjFd8ri2KbXWr_x4/sendMessage?chat_id=-781132811&text='+text)


def print_init_state(val):
    if val == 0:
        return "off"
    else:
        return "on"


def perform_operation_humidity(hum):
    global hum_act_state
    if hum > HUM_LIM and hum_act_state == 0:
        hum_act_state = 1
        send_telegram_notification("Humidity actuator " + TOPIC.replace("mtds/sensor/data/", "").replace("/", ".") + " on - Hum is " + str(hum))
        print("Humidity actuator on - Hum is " + str(hum))
    elif hum <= HUM_LIM and hum_act_state == 0:
        print("Humidity no changes")
    elif hum < HUM_LIM and hum_act_state == 1:
        hum_act_state = 0
        send_telegram_notification(
            "Humidity actuator " + TOPIC.replace("mtds/sensor/data/", "").replace("/", ".") + " off - Hum is " + str(
                hum))
        print("Humidity actuator off - Hum is " + str(hum))
    elif hum >= HUM_LIM and hum_act_state == 1:
        print("Humidity no changes")


def perform_operation_temperature(temp):
    global temp_act_state
    if temp > TEMP_LIM and temp_act_state == 0:
        temp_act_state = 1
        send_telegram_notification(
            "Temperature actuator " + TOPIC.replace("mtds/sensor/data/", "").replace("/", ".") + " on - Temp is " + str(
                temp))
        print("Temperature actuator on - Temp(°C) is " + str(temp))
    elif temp <= TEMP_LIM and temp_act_state == 0:
        print("Temperature no changes")
    elif temp < TEMP_LIM and temp_act_state == 1:
        temp_act_state = 0
        send_telegram_notification(
            "Temperature actuator " + TOPIC.replace("mtds/sensor/data/", "").replace("/", ".") + " off - Temp is " + str(
                temp))
        print("Temperature actuator off - Temp(°C) is " + str(temp))
    elif temp >= TEMP_LIM and temp_act_state == 1:
        print("Temperature no changes")

def parse_incoming_message(message):
    data = json.loads(message)
    hum = data['d']['hum']
    temp = data['d']['temp_c']
    perform_operation_temperature(temp)
    perform_operation_humidity(hum)


def on_connect(client, userdata, flags, rc):
    print("Connected with result code {0}".format(str(rc)))
    print("Temperature actuator is " + print_init_state(temp_act_state))
    print("Humidity actuator is " + print_init_state(hum_act_state))
    client.subscribe(TOPIC)


def on_message(client, userdata, msg):
    parse_incoming_message(msg.payload.decode())


client = mqtt.Client("mtds-actuator-0001")
client.on_connect = on_connect
client.on_message = on_message
client.connect(BROKER, PORT)
client.loop_forever()