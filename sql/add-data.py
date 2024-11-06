import random
from datetime import datetime, timedelta
import sys
import psycopg2 as psql

cnx = psql.connect(user='postgres', database='mk_test', port=5433)

cursor = cnx.cursor()

symbol = sys.argv[1] if len(sys.argv) > 1 else 'BTC'

price = 40000.0
change_multiplier = 100

now_time = datetime.now()
time_iter = now_time - timedelta(days=90)

insert_query_stmt = 'insert into crypto_price (symbol, ts, ltp) values {}'

rows = []
idx = 0
while time_iter < now_time:
	change_factor = random.uniform(-1, 1)
	price = price + change_factor*change_multiplier
	time_iter += timedelta(seconds=1)
	if idx <= 1000:
		idx += 1
		rows.append(("{}".format(symbol), "{}".format(time_iter.strftime('%Y-%m-%dT%H:%M:%SZ')), price))
	if idx == 1000:
		cursor.execute(insert_query_stmt.format(','.join(list(map(lambda x: x.__str__(), rows)))))
		rows = []
		idx = 0

if len(rows) > 0:
	cursor.execute(insert_query_stmt.format(','.join(list(map(lambda x: x.__str__(), rows)))))

cursor.execute('COMMIT')
cursor.close()
cnx.close()
