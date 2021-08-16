from time import sleep
from json import dumps
from kafka import KafkaProducer
import time
import random
from sqlalchemy import create_engine


class Producer:
    
    def __init__(self):
        self.producer = KafkaProducer(bootstrap_servers=['localhost:9092'], value_serializer=lambda m: dumps(m).encode('ascii'))

    def emit(self, cust=55, type="dep"):
        engine = create_engine('sqlite:///bank.db')
        db = engine.connect()
        sql = f"SELECT count() FROM customer"
        current_count = db.execute(sql).fetchall()
        # get count of number of customers, and then execute a transaction from one of the customers
        # in the database
        #print('hello', current_count[0][0])
        #print(current_count)
<<<<<<< HEAD
        
=======
        xaction_type = self.depOrWth()
        amount = random.randint(10,101)*100
        if xaction_type == 'wth':
            amount = -amount
>>>>>>> bcd45796d920217ea1b157503a35985689e35128
        data = {
            'custid' : random.randint(1, current_count[0][0]),
            'branchid': random.randint(1, 3),
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
<<<<<<< HEAD
            partition = int(argv[1])
            self.producer.send('transactions', partition=partition, value=data)
            sleep(1)
            
=======
            self.producer.send('transactions', partition=data['branchid']-1, value=data)
            sleep(.5)
>>>>>>> bcd45796d920217ea1b157503a35985689e35128
            

if __name__ == "__main__":
    sleep(5)
    p = Producer()
    p.generateRandomXactions(n=1000)
    # by passing n = 20, it overwrites n = 1000 default 
