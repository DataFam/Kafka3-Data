from time import sleep
from json import dumps
from kafka import KafkaProducer
import time
import random
from sqlalchemy import create_engine
from sys import argv


class Producer:
    
    def __init__(self, partition):
        self.partition = partition
        self.producer = KafkaProducer(bootstrap_servers=['localhost:9092'], value_serializer=lambda m: dumps(m).encode('ascii'))

    def emit(self, cust=55, type="dep"):
        engine = create_engine('sqlite:///bank.db')
        db = engine.connect()
        sql = f"SELECT count() FROM customer"
        current_count = db.execute(sql).fetchall()
        # get count of number of customers, and then execute a transaction from one of the customers
        # in the database
        xaction_type = self.depOrWth()
        amount = random.randint(10,101)*100
        if xaction_type == 'wth':
            amount = -amount
        data = {
            'custid' : random.randint(1, current_count[0][0]),
            'branchid': self.partition + 1,
            'type': xaction_type,
            'date': int(time.time()),
            'amt': amount,
            }
        return data
    
    def depOrWth(self):
        return 'dep' if (random.randint(0,2) == 0) else 'wth'

    def generateRandomXactions(self, n=1000):
        # n defaults to 1000
        for _ in range(n):
            data = self.emit()
            print('sent', data)
            self.producer.send('transactions', partition=self.partition, value=data)
            sleep(.5)
            

if __name__ == "__main__":
    partition = int(argv[1])
    sleep(5)
    p = Producer(partition)
    p.generateRandomXactions(n=1000)
    # by passing n = 20, it overwrites n = 1000 default 
