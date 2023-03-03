import sys
sys.path.insert(1, '//'.join((sys.path[0]).split('\\')[:-1]))

from giraldi_backend import *
import yfinance as yf

pd.options.display.float_format = '{:.2f}'.format

start = '2023-01-01'

fields = {
    'Adj Close': 9,
    'Close': 1,
    'Open': 2,
    'Low': 3,
    'High': 4,
    'Volume': 8
}

sql = f'''
    SELECT ticker FROM tickers
    WHERE ticker <> '^SPX'
'''
tickers = pd.read_sql_query(sql, engine)
tickers = tickers.iloc[:,0].to_list()

df = yf.download(tickers, start=start)

df.reset_index(inplace=True)
df = pd.melt(df, id_vars=['Date'])

df.columns = ['rptdt', 'field_code', 'ticker', 'value']
df['field_code'] = df['field_code'].apply(lambda x: fields[x])

df = df[['rptdt', 'ticker', 'field_code', 'value']]
df.dropna(inplace=True)

sql = f'''
    DELETE FROM stock_quotes
    WHERE rptdt >= '01-Jan-2023'
'''
engine.execute(sql)

df.to_sql('stock_quotes', engine, index=False, if_exists='append')

logger.success(f'finished inserting {len(df)} rows')