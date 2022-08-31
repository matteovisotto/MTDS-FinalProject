import paho.mqtt.client as mqtt

BROKER = "server.matmacsystem.it"
PORT = 1883
TOPIC = "mtds/sensor/data/A/1/S/1"

global hum_act_state
hum_act_state = 0

global temp_act_state
temp_act_state = 0


def perform_operation_humidity(hum):
    global  hum_act_state
    if hum > 10 and hum_act_state == 0:
        hum_act_state = 1
        print("Humidity actuator on")
    elif hum <= 10 and hum_act_state == 0:
        print("Humidity no changes")
    elif hum < 10 and hum_act_state == 1:
        hum_act_state = 0
        print("Humidity actuator off")
    elif hum >= 10 and hum_act_state == 1:
        print("Humidity no changes")

def perform_operation_temperature(temp):
    global temp_act_state
    if temp > 10 and temp_act_state == 0:
        temp_act_state = 1
        print("Temperature actuator on")
    elif temp <= 10 and temp_act_state == 0:
        print("Temperature no changes")
    elif temp < 10 and temp_act_state == 1:
        temp_act_state = 0
        print("Temperature actuator off")
    elif temp >= 10 and temp_act_state == 1:
        print("Temperature no changes")


def on_connect(client, userdata, flags, rc):
    print("Connected with result code {0}".format(str(rc)))
    client.subscribe(TOPIC)


def on_message(client, userdata, msg):
    perform_operation_temperature(int(msg.payload.decode()))


client = mqtt.Client("mtds-actuator-0001")
client.on_connect = on_connect
client.on_message = on_message
client.connect(BROKER, PORT)
client.loop_forever()