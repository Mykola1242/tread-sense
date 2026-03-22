from paho.mqtt import client as mqtt_client
import time
from schema.aggregated_data_schema import AggregatedDataSchema
from schema.parking_schema import ParkingSchema  
from file_datasource import FileDatasource
import config

def connect_mqtt(broker, port):
    print(f"CONNECT TO {broker}:{port}")
    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            print(f"Connected to MQTT Broker ({broker}:{port})!")
        else:
            print(f"Failed to connect, return code {rc}")
            exit(rc)

    client = mqtt_client.Client()
    client.on_connect = on_connect
    client.connect(broker, port)
    client.loop_start()
    return client

def publish(client, topic, datasource, delay): 
    while True: 
        time.sleep(delay) 
        data = datasource.read() 
        road_msg = AggregatedDataSchema().dumps(data)
        client.publish(topic, road_msg)

        parking_topic = getattr(config, 'MQTT_PARKING_TOPIC', 'parking_data_topic')
        parking_msg = ParkingSchema().dumps(data.parking)
        client.publish(parking_topic, parking_msg)

        print(f"Data sent to `{topic}` and `{parking_topic}`")

def run(): 
    client = connect_mqtt(config.MQTT_BROKER_HOST, config.MQTT_BROKER_PORT)
    
    datasource = FileDatasource(
        "data/accelerometer.csv", 
        "data/gps.csv", 
        "data/parking.csv"
    )
    
    datasource.startReading()
    
    try:
        publish(client, config.MQTT_TOPIC, datasource, config.DELAY)
    except KeyboardInterrupt:
        datasource.stopReading()
        print("Stopped")

if __name__ == '__main__':
    run()