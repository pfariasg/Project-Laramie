import sys
sys.path.insert(1, '//'.join((sys.path[0]).split('\\')[:-1]))

from giraldi_backend import *
import yfinance as yf

pd.options.display.float_format = '{:.2f}'.format

dull = True

start = '2023-01-01'


def fix_currency_ticker(ticker):
    if ticker.endswith('=X'):
        ticker = ticker[:-2]
        if ticker[:3] in ('EUR', 'AUD', 'GBP', 'NZD'):
            return ticker
        else:
            return f'USD{ticker}'
    else:
        return ticker


# fields = {
#     'Adj Close': 9,
#     'Close': 1,
#     'Open': 2,
#     'Low': 3,
#     'High': 4,
#     'Volume': 8
# }

sql = f'''
    SELECT ticker, asset_type FROM tickers
    WHERE ticker <> '^SPX'
'''
tickers = pd.read_sql_query(sql, engine)
tickers['ticker'] = tickers.apply(lambda x: f"{x['ticker']}.SA" if x['asset_type'].startswith('bz_')==True else x['ticker'], axis=1)

tickers = tickers.iloc[:,0].to_list()

df = yf.download(tickers, start=start)['Close']
df.reset_index(inplace=True)
df = pd.melt(df, id_vars=['Date'])

df.columns = ['rptdt', 'ticker', 'value']
df['field_code'] = 1

df = df[['rptdt', 'ticker', 'field_code', 'value']]
df['ticker'] = df['ticker'].apply(fix_currency_ticker)
df['ticker'] = df['ticker'].str.replace('.SA', '', regex=False)
df['rptdt'] = df['rptdt'].dt.date
df.dropna(inplace=True)

df = df.loc[df.value!=0]

sql = f'''
    DELETE FROM quotes
    WHERE rptdt >= '01-Jan-2023'
'''
if not dull:
    engine.execute(sql)

    df.to_sql('quotes', engine, index=False, if_exists='append')
df.to_clipboard()
logger.success(f'finished inserting {len(df)} rows')