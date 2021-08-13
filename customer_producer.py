from random import randint, choice, random, uniform
from kafka import KafkaProducer
from time  import sleep, time
import uuid
import sqlite3
from json import dumps

from sqlalchemy.sql.operators import custom_op

def customer_transaction_generator(producer):


    listOfFirstNames = ['Sean', 'Wes', 'Aidan', 'Chuck', 'Nikki', 'Kristopher', 'Lossie', 'Desa', 'Leon', 'Dolio']
    listOfLastNames = ['Lan', 'Thorpe', 'Farhi', 'Kelly', 'Wojo', 'Younger', 'Freeman', 'Burton', 'Hunter', 'Durant']

    # uuid.int()
    for i in range(20):
        data = {}
        data['f_name'] = choice(listOfFirstNames)
        data['l_name'] = choice(listOfLastNames)
        data['date'] = int(time())
        producer.send('new_customers', value=data)
        print('customer added')
        sleep(5)

if __name__ == '__main__':
     producer = KafkaProducer(bootstrap_servers=['localhost:9092'], value_serializer=lambda m: dumps(m).encode('ascii'))
     customer_transaction_generator(producer)