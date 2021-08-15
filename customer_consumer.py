from kafka import KafkaConsumer, TopicPartition
from json import loads
from sqlalchemy import create_engine, Column, Integer, String
from sqlalchemy.orm import declarative_base

if __name__ == "__main__":
    engine = create_engine('sqlite:///bank.db', echo = True)
    db = engine.connect()
    consumer = KafkaConsumer('new_customers',
            bootstrap_servers=['localhost:9092'],
            value_deserializer=lambda m: loads(m.decode('ascii')))

    for message in consumer: 
            #print(message.value)
            f_name, l_name, date, balance = message.value.values()
            
            db.execute("INSERT INTO customer (createdate, fname, lname, balance) VALUES (?, ?, ?, ?)",
                (date, f_name, l_name, balance))
            


    