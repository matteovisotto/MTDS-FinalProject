import paho.mqtt.client as mqtt

BROKER = "server.matmacsystem.it"
PORT = 1883

N = ["A", "B", "C", "D"]
B = ["0", "1", "2", "3"]
F = ["S", "0", "1", "2"]

def get_sensor_location(sensor_id):
    s = sensor_id.replace("mtdssens-", "")
    loc = N[int(s[0])] + "/" + B[int(s[1])] + "/" + F[int(s[2])] + "/" + s[3]
    return loc

def on_sensor_request_config(sensor_id):
    publish_client = mqtt.Client("control1")
    publish_client.connect(BROKER, PORT)
    publish_client.publish("mtds/sensor/conf/"+sensor_id, get_sensor_location(sensor_id))


def on_connect(client, userdata, flags, rc):
    print("Connected with result code {0}".format(str(rc)))
    client.subscribe("mtds/sensor/conf")


def on_message(client, userdata, msg):
    sensor_id = msg.payload.decode()
    on_sensor_request_config(sensor_id)

client = mqtt.Client("mtds-sensor-configurator")
client.on_connect = on_connect
client.on_message = on_message
client.connect(BROKER, PORT)
client.loop_forever()