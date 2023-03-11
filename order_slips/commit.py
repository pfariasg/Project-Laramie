import sys
sys.path.insert(1, '//'.join((sys.path[0]).split('\\')[:-1]))

from giraldi_backend import *

dull = False

df = pd.read_excel(r'C:/Projects/Macroboys Capital/order_slips/input.xlsx', usecols='A:L')

sql = f'''
    SELECT ticker, name, asset_type FROM tickers
    '''
mapped_yahoo = pd.read_sql_query(sql, engine)
mapped_yahoo['ticker'] = mapped_yahoo.apply(lambda x: x['name'] if x['asset_type']=='fx' else x['ticker'], axis=1)

unmapped = df[['ticker', 'asset_type']].merge(mapped_yahoo, how='left')
unmapped = unmapped.loc[unmapped['asset_type'].isin(('bz_stock', 'bz_etf', 'us_stock', 'us_etf', 'fx')), :]

unmapped = unmapped[unmapped['name'].isna()]

if not unmapped.empty:
    logger.error(f'found unmapped tickers: ')
    print(unmapped)
    logger.error(f'no changes were made')
    sys.exit()

sql = '''DELETE FROM TRADES
        WHERE ticker = NULL'''

if not dull:
    df.to_sql('trades', engine, index=False, if_exists='append')
    engine.execute(sql)

logger.success('trades committed')