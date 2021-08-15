from kafka import KafkaConsumer, TopicPartition
from json import loads
from sys import argv
from sqlalchemy import create_engine, Column, Integer, String
from sqlalchemy.orm import declarative_base

if __name__ == '__main__':
    engine = create_engine('sqlite:///bank.db', echo = True)
    db = engine.connect()
    consumer = KafkaConsumer(
        bootstrap_servers=['localhost:9092'],
        value_deserializer=lambda m: loads(m.decode('ascii')))

    partition = int(argv[1])
    consumer.assign([TopicPartition('new_customers', partition)])

    for message in consumer: 
            #print(message.value)
            f_name, l_name, date, balance = message.value.values()
            
            db.execute("INSERT INTO customer (createdate, fname, lname, balance) VALUES (?, ?, ?, ?)",
                (date, f_name, l_name, balance))
            


    