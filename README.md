# Project Steps

1. Create `init_db.py` script that drops all tables and re-creates them

2. Create Transaction Table (create table in `init_db.py` using sqlalchemy)

3. Create Customer Table (create table in `init_db.py` using sqlalchemy)

4. Create Healthy-ish balances Table (create table in `init_db.py` using sqlalchemy)

5. Create Customer Producer and Consumer

6. Create Transaction Consumer

7. Create Summary Consumers

8. Create Limit Consumer

9. Handle multiple branches and partitions

10. Create a spark job

# Running the beast

## First get Kafka up and running on your machine

`docker-compose up`

## Then create database (if it doesn't exist)

`touch bank.db`

`sqlite3 bank.db`
- initializes bank.db as a sqlite3 database

`.quit`

## Populate the database with the correct tables

`python3 init_db.py`

## Now create your topics, groupid's and partitions

1. Create a topics 'transactions' and 'customers' each with 3 partitions (you can do this from Kafdrop)

2. Create a 'summaries' and 'delinquents' groupid for the 'transactions' topic. You have to do this from the command line. If the method from shell does not work, try the method from inside docker conatiner.

### Method from shell

- `kafka-console-consumer --bootstrap-server localhost:9092 --topic transactions --group delinquent`

- `kafka-console-consumer --bootstrap-server localhost:9092 --topic transactions --group summaries`

### Method from inside docker container:

- `docker exec -it dataengineeringkafkambyn_kafka_1 bash`

- `cd /opt/kafka/bin`

- `./kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic transactions --group delinquent`

- `./kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic transactions --group summaries` 

## LAST STEP -- START THE BEAST

- `chmod +x start_the_beast.sh` (you only need to do this once)

- `./start_the_beast.sh`

## To stop the madness:

- go to your activity monitor, find everything named python and force quit it.