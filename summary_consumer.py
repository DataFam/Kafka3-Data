from kafka import KafkaConsumer
from json import loads
from sqlalchemy import create_engine
import statistics

if __name__ == "__main__":
    engine = create_engine('sqlite:///bank.db', echo = True)
    db = engine.connect()
    consumer = KafkaConsumer('transactions',
            bootstrap_servers=['localhost:9092'],
            group_id='summaries',
            value_deserializer=lambda m: loads(m.decode('ascii')))

    # Create two empty lists, one to hold withdrawals and the other to hold deposits (outside of for loop, otherwise they would be reset each iteration)
    # Zeros are added as placeholders so average/ stdev functions still work if no withdrawals/deposits have been made yet
    # Note: std dev formula needs at least two numbers to be calculated, which is why I added two zeros instead of just one
    withdrawals = [0,0]
    deposits = [0,0]

    for message in consumer: 
            custid, branchid, xaction_type, date, amt = message.value.values()

            # Removing the placeholder if it's still there so that it doesn't effect average/ stdev
            # Adding the transaction amount to the appropriate list
            if xaction_type == 'wth':
                if withdrawals[0] == 0:
                    withdrawals.remove(0)
                amount = float(amt)
                withdrawals.append(amount)
            elif xaction_type == 'dep':
                if deposits[0] == 0:
                    deposits.remove(0)
                amount = float(amt)
                deposits.append(amount)

            # Finding the average / standard deviation of each list, then printing the results
            avg_wth = round(statistics.mean(withdrawals),2)
            std_wth = round(statistics.stdev(withdrawals),2)
            avg_dep = round(statistics.mean(deposits),2)
            std_dep = round(statistics.stdev(deposits), 2)

            print("Average Withdrawals: ", avg_wth)
            print("Std Dev Withdrawals: ", std_wth)
            print("Average Deposits: ", avg_dep)
            print("Std Dev Deposits: ", std_dep)