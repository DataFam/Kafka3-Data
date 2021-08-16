#!/usr/bin/env bash

echo "...Starting the beast..."
python3 customer_consumer.py 0 & 
python3 customer_consumer.py 1 & 
python3 customer_consumer.py 2 & 
python3 customer_producer.py 0 & 
python3 customer_producer.py 1 &
python3 customer_producer.py 2 &
python3 transaction_con_prod.py 0 &
python3 transaction_con_prod.py 1 & 
python3 transaction_con_prod.py 2 &
python3 producer-random-xactions.py 0 &
python3 producer-random-xactions.py 1 &
python3 producer-random-xactions.py 2 &
python3 limit_consumer.py &
python3 summary_consumer.py &