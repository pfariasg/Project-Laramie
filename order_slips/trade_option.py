import sys
sys.path.insert(1, '//'.join((sys.path[0]).split('\\')[:-1]))

import datetime
import yfinance as yf
import pandas as pd
pd.set_option('display.max_rows', 100)

from giraldi_backend import *

underlying = 'AAPL'
option = 'call'

quantity = -1000
trade_id = None

fund = 'laramie'
book = 'macro'
trader = 'pgiraldi'

obs = 'NULL'

dull = True

##############################################
if obs != 'NULL':
    obs = f'"{obs}"'

ticker = yf.Ticker(underlying)

expirations = [datetime.datetime.strptime(date, "%Y-%m-%d").strftime("%d-%b-%Y") for date in ticker.options]

groups = []
for date in expirations:
    if date[3:] not in groups:
        groups.append(date[3:])

cur_year = groups[0][-4:]
print('-----------')
for group in groups:
    if cur_year != group[-4:]:
        cur_year = group[-4:]
        print('-----------')

    chosen = [date for date in expirations if date[3:]==group]
    print('   '.join(chosen))
print('-----------')

expiration = input('Choose an expiration date: ')
expiration = datetime.datetime.strptime(expiration, '%d-%b-%Y')

if option == 'call':
    chain = ticker.option_chain(date=expiration.strftime('%Y-%m-%d')).calls
elif option == 'put':
    chain = ticker.option_chain(date=expiration.strftime('%Y-%m-%d')).puts

chain['bid-ask_spread'] = chain['ask'] - chain['bid']

if quantity < 0:
    chain.rename({'ask': 'price'}, axis=1, inplace=True)
elif quantity > 0:
    chain.rename({'bid': 'price'}, axis=1, inplace=True)

currency = chain.iloc[0]['currency']
chain = chain[['strike', 'inTheMoney', 'lastPrice', 'price', 'bid-ask_spread', 'volume', 'openInterest']]

print('\n', chain)

strike = float(input('\n---------\nChoose a strike: '))

chain = chain.loc[chain['strike']==strike]

if chain.iloc[0]['price'] == 0:
    logger.error('Option currently not being traded')
    sys.exit()

ticker = f"{option[0].upper()}_{underlying.upper()}_{round(strike, 2)}_{expiration.strftime('%Y-%b-%d').upper()}"
print(f'----------\nSelected ticker {ticker}\n----------')

sql = f'''
    SELECT * FROM options
    WHERE ticker = '{ticker}'
'''
df = pd.read_sql_query(sql, engine)

# if option not mapped in database 
if df.empty:
    sql = f'''
        SELECT max(id) FROM options
    '''
    code = pd.read_sql_query(sql, engine)
    code = code.values[0][0] + 1
    
    sql = f'''
        INSERT INTO options (id, ticker, underlying, expiration, strike, type, currency)
        VALUES ({code}, '{ticker}', '{underlying}', '{expiration.strftime('%Y-%b-%d').upper()}', {round(strike, 2)}, 'european', '{currency}')
    '''
    if not dull:
        engine.execute(sql)
    logger.info(f'''inserted {ticker} into options, code {code}''')
else:
    logger.info(f'Ticker {ticker} already tracked in database')

trade_id = get_trade_id(trade_id)

# commit trade to database
sql = f'''
    INSERT INTO trades (rptdt, trade_id, fund, book, trader, ticker, asset_type, quantity, price, currency, timestamp, obs)
    VALUES ('{datetime.date.today().strftime("%d-%b-%Y")}', {trade_id}, '{fund}', '{book}', '{trader}', '{ticker}', 'option', {quantity}, {chain.iloc[0]['price']}, 'USD', '{datetime.datetime.now().strftime('%d-%b-%Y %I:%M:%S %p')}', {obs})
'''
if not dull:
    engine.execute(sql)
logger.success(f'Trade completed: {quantity} {ticker} at {chain.iloc[0]["price"]}')

