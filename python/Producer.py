from opensky_api import OpenSkyApi
from kafka import KafkaProducer
import time
import json
Producer = KafkaProducer(bootstrap_servers="localhost:9092",value_serializer=lambda v: json.dumps(v).encode('utf-8'))
api = OpenSkyApi("Firass_jendoubi","firas123")
s = api.get_states()
while True:
    for i in s.states:
        d={"coord":[i.longitude,i.latitude],"country":str(i.origin_country),"time":str(i.time_position)}
        print(d)
        Producer.send("flightsdata",d)

    print("Data sended to the producer")
    api = OpenSkyApi("Firass_jendoubi","firas123")
    s = api.get_states()
    time.sleep(1)

