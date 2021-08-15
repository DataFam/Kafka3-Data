from random import choice
from kafka import KafkaProducer
from time  import sleep, time
from json import dumps
from sys import argv


def customer_generator(producer, partition):

    listOfFirstNames = ['Sean', 'Wes', 'Aidan', 'Chuck', 'Nikki', 'Kristopher', 'Lossie', 'Desa', 'Leon', 'Dolio']
    listOfLastNames = ['Lan', 'Thorpe', 'Farhi', 'Kelly', 'Wojo', 'Younger', 'Freeman', 'Burton', 'Hunter', 'Durant']

    for i in range(20):
        data = {}
        data['f_name'] = choice(listOfFirstNames)
        data['l_name'] = choice(listOfLastNames)
        data['date'] = int(time())
        data['balance'] = 0
        producer.send('new_customers', partition=partition, value=data)
        print('customer added to partition', partition)
        sleep(1)


if __name__ == '__main__':

    # optionally take in partition number from command line
    partition = choice([0, 1, 2])
    if len(argv) == 2:
        partition = int(argv[1])

    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'], 
        value_serializer=lambda m: dumps(m).encode('ascii')
    )
    # pass in partition for producer to start producing to
    customer_generator(producer, partition)
