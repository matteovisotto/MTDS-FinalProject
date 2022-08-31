import paho.mqtt.client as paho

BROKER = "server.matmacsystem.it"
PORT = 1883
TOPIC = "mtds/sensor/data/A/1/S/2"
DATA = "200"

def on_publish(client,userdata,result):
    print("data published \n")
    pass


mqtt_client= paho.Client("mtds-test-publish-client")
mqtt_client.on_publish = on_publish
mqtt_client.connect(BROKER,PORT)
ret = mqtt_client.publish(TOPIC, DATA)