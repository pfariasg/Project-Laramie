import sys
sys.path.insert(1, '//'.join((sys.path[0]).split('\\')[:-1]))

import datetime
from giraldi_backend import *

######################################################################

ticker = 'ODF24'
quantity = 10
price = 13.5
trade_id = None

fund = 'laramie'
book = 'macro'
trader = 'pgiraldi'

obs = 'NULL'

dull = True

######################################################################
if obs != 'NULL':
    obs = f'"{obs}"'

asset_type = get_asset_type(ticker)
trade_id = get_trade_id(trade_id)

# send order to database
sql = f'''
    INSERT INTO trades (rptdt, trade_id, fund, book, trader, ticker, asset_type, quantity, price, currency, timestamp, obs)
    VALUES ('{datetime.date.today().strftime("%d-%b-%Y")}', {trade_id}, '{fund}', '{book}', '{trader}', '{ticker}', '{asset_type}', {quantity}, {price}, 'USD', '{datetime.datetime.now().strftime('%d-%b-%Y %I:%M:%S %p')}', {obs})
'''
if not dull:
    engine.execute(sql)
logger.success(f'Trade #{trade_id} completed: {quantity} {ticker} at {price}')