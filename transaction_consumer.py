from kafka import KafkaConsumer
from json import loads
from sqlalchemy import create_engine

if __name__ == "__main__":
    engine = create_engine('sqlite:///bank.db', echo = True)
    db = engine.connect()
    consumer = KafkaConsumer('transactions',
            bootstrap_servers=['localhost:9092'],
            value_deserializer=lambda m: loads(m.decode('ascii')))

    for message in consumer: 
            print(message.value)
            custid, branchid, xaction_type, date, amt = message.value.values()
            sql = f"INSERT INTO \"transaction\"(branchid, custid, type, date, amt) VALUES({branchid}, {custid}, '{xaction_type}', {date}, {amt})"
            db.execute(sql)