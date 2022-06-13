from time import sleep, time, ctime
from random import random, randint, choice
from kafka import KafkaProducer
from SimpleLogEngine import log

def kafka_stream_simulator():
    """
    Simulates streaming Kafka data.
    """

    log(f'kafka_stream_simulator,None,Randomly generated traffic data messages,{__file__}')

    # creates producer with topic=toll.
    producer = KafkaProducer(bootstrap_servers='localhost:9092')

    TOPIC = 'toll'

    # streaming data simulator:
    VEHICLE_TYPES = ("car", "car", "car", "car", "car", "car", "car", "car",
                     "car", "car", "car", "truck", "truck", "truck",
                     "truck", "van", "van")
    for _ in range(100000):
        vehicle_id = randint(10000, 10000000)
        vehicle_type = choice(VEHICLE_TYPES)
        now = ctime(time())
        plaza_id = randint(4000, 4010)
        message = f"{now},{vehicle_id},{vehicle_type},{plaza_id}"
        message = bytearray(message.encode("utf-8"))
        print(f"A {vehicle_type} has passed by the toll plaza {plaza_id} at {now}.")
        producer.send(TOPIC, message)
        sleep(random() * 2)

kafka_stream_simulator()