import sys
sys.path.insert(1, '//'.join((sys.path[0]).split('\\')[:-1]))

import datetime
import pytz
import yfinance as yf
from giraldi_backend import *

######################################################################

ticker = 'AAPL'
quantity = 1000
trade_id = None

fund = 'laramie'
book = 'macro'
trader = 'pgiraldi'

obs = 'NULL'

dull = True

######################################################################
if obs != 'NULL':
    obs = f'"{obs}"'

# get price and check if it's recent
df = yf.download(ticker, start=datetime.date.today()-datetime.timedelta(days=3), interval='1m')

if df.empty:
    logger.error(f'Security {ticker} is currently not being traded')
    sys.exit()

date = datetime.datetime.fromtimestamp(df.index[-1].timestamp(),pytz.timezone('US/Eastern')).replace(tzinfo=None)
diff = (datetime.datetime.now()-date).total_seconds()

if diff > 70:
    logger.error(f'Security {ticker} is currently not being traded')
    sys.exit()

price = round(df['Close'][-1],2)

trade_id = get_trade_id(trade_id)
asset_type = get_asset_type(ticker)

# send order to database
sql = f'''
    INSERT INTO trades (rptdt, trade_id, fund, book, trader, ticker, asset_type, quantity, price, currency, timestamp, obs)
    VALUES ('{datetime.date.today().strftime("%d-%b-%Y")}', {trade_id}, '{fund}', '{book}', '{trader}', '{ticker}', '{asset_type}', {quantity}, {price}, 'USD', '{datetime.datetime.now().strftime('%d-%b-%Y %I:%M:%S %p')}', {obs})
'''
if not dull:
    engine.execute(sql)
logger.success(f'Trade #{trade_id} completed: {quantity} {ticker} at {price}')
