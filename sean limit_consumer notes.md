## in case I'm not awake when everyone else is
- I don't think I did it the way the readme suggested to modify classes, but I looked at a differnt way.
- the problem asks to create a consumer if any customer's balances are below a certain level, so i thought to create a balance column that would let the system know if anyone is below -5000 in their account

### init_db.py
- added transaction.custid a foreign key of customer.custid
- made a balance column in customer
- created a new table, healthy-ish balances, (sarcastic name)
- healthy-ish balances does not have a 

### customer_producer.py
- every new customer gets a starting balance of 0 

### customer_consumer.py
- added support for starting 'balance'

### producer-random-xactions.py
- now when a transaction is generated, a random primary key (currently using randint of SQL count statement, but should probably use

'''
SELECT column FROM table
ORDER BY RAND()
LIMIT 1

'''

is added to every transaction

### trnsaction_con_prod.py
- renamed from transaction_consumers.py
- is now a consumer AND a producer
- consumer updates balance values in customer's table
- producer takes 'delinquents' (< -5000 in balance) and sends to 'delinquents' topic. 

### limit_consumer.py
- limit_consumer.py takes messages from the 'delinquents' topic 
- prints a simple message of 'id is a delinquent'
- then stores in the new healthy-ish balances table
- not fully tested up till this point, also just added a primary key to the healthy-ish table, in init_db.py so please check if that's ifne. (I don't think this affects the work though)


