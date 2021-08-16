from kafka import KafkaConsumer, KafkaProducer, TopicPartition
from json import loads, dumps
from sqlalchemy import create_engine
from random import randint
from sys import argv


def transaction_consumer_producer(partition):
        engine = create_engine('sqlite:///bank.db', echo = True)
        db = engine.connect()
        consumer = KafkaConsumer(
                bootstrap_servers=['localhost:9092'],
                value_deserializer=lambda m: loads(m.decode('ascii')),
                group_id = 'delinquent'
                )
        consumer.assign([TopicPartition('transactions', partition)])

        for message in consumer: 
                print(message.value)
                custid, branchid, xaction_type, date, amt = message.value.values()
                update_consumer = f"INSERT INTO \"transaction\"(branchid, custid, type, date, amt) VALUES({branchid}, {custid}, '{xaction_type}', {date}, {amt})"
                db.execute(update_consumer)
                if xaction_type == 'wth':
                        current = wth(amt, custid)
                        if current:
                                if current < -5000:
                                        delinquent_producer(custid, date, xaction_type, current)
                                
                elif xaction_type == 'dep':
                        current = dep(amt, custid)
                        if current:
                                if current < -5000:
                                        delinquent_producer(custid, date, xaction_type, current)
def wth(amt, custid):
        engine = create_engine('sqlite:///bank.db', echo = True)
        db = engine.connect()
        withdrawal = "UPDATE \'customer\' SET balance = balance + {} WHERE custid = {}".format(amt, custid)
        db.execute(withdrawal)
        current_sql = "SELECT balance FROM \'customer\' WHERE custid = {}".format(custid)
        current = db.execute(current_sql).fetchall()
        if len(current) == 0:
                return None
        return current[0][0]


def dep(amt, custid):
        engine = create_engine('sqlite:///bank.db', echo = True)
        db = engine.connect()
        deposit = "UPDATE \'customer\' SET balance = balance + {} WHERE custid = {}".format(amt, custid)
        db.execute(deposit)
        current_sql = "SELECT balance FROM \'customer\' WHERE custid = {}".format(custid)
        current = db.execute(current_sql).fetchall()
        if len(current) == 0:
                return None
        return current[0][0]


def delinquent_producer(custid, date, xaction, current):
        producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
        value_serializer=lambda m: dumps(m).encode('ascii'))
        data = {}
        data['custid'] = custid
        data['date'] = date
        data['xaction'] = xaction
        data['current'] = current
        producer.send('delinquents', value = data)
        print('delinquent added')
        
if __name__ == "__main__":
        partition = int(argv[1])
        transaction_consumer_producer(partition)
