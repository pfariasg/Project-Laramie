import sys
sys.path.insert(1, '//'.join((sys.path[0]).split('\\')[:-1]))

import datetime
import yfinance as yf
import pandas as pd
# from giraldi_pricing import *
from giraldi_backend import *

def get_tickers():
    sql = '''
        SELECT * FROM tickers
        '''
    df = pd.read_sql_query(sql, engine)
    df['ticker'] = df.apply(lambda x: x['ticker']+'.SA' if x['asset_type']=='br_stock' else x['ticker'], axis=1)

    return df['ticker'].tolist()

def query(opt, underlying):
    sql = f'''
        SELECT * FROM options
        WHERE ticker = '{opt["ticker"]}'
    '''
    df = pd.read_sql_query(sql, engine)
    if df.empty:
        sql = f'''
            SELECT max(id) FROM options
        '''
        code = pd.read_sql_query(sql, engine)
        code = code.values[0][0] + 1

        sql = f'''
            INSERT INTO options (id, ticker, underlying, expiration, strike, type, currency)
            VALUES ({code}, '{opt["ticker"]}', '{underlying}', '{opt["exp"].strftime("%d-%b-%Y")}', {opt["strike"]}, 'european', '{opt["currency"]}')
        '''
        engine.execute(sql)
        logger.info(f'''inserted {opt['ticker']} into options, code {code}''')
        return code
        
    elif len(df) == 1:
        return df['id'][0]

if __name__ == '__main__':
    count = 0
    
    for asset in get_tickers():
        ticker = yf.Ticker(asset)
        
        if not ticker.options:
            logger.info(f'{asset} has no options')
            continue
        
        # create dataframe with all strikes and expirations
        for i, expiration in enumerate(ticker.options):

            chain = ticker.option_chain(date=expiration)

            calls = chain.calls

            calls = calls.loc[:, ['strike', 'lastPrice', 'bid', 'ask', 'openInterest', 'volume', 'currency']]
            calls.columns = ['strike', 'close', 'bid', 'ask', 'open_interest', 'volume', 'currency']
            
            calls['exp'] = datetime.datetime.strptime(expiration, '%Y-%m-%d')
            calls['call_put'] = 'C'

            if i == 0:
                df = calls.copy()
            else:
                df = pd.concat([df, calls], axis=0, ignore_index=True)

            puts = chain.puts
            puts = puts.loc[:, ['strike', 'lastPrice', 'bid', 'ask', 'openInterest', 'volume', 'currency']]
            puts.columns = ['strike', 'close', 'bid', 'ask', 'open_interest', 'volume', 'currency']
            puts['exp'] = datetime.datetime.strptime(expiration, '%Y-%m-%d')
            puts['call_put'] = 'P'

            df = pd.concat([df, puts], axis=0, ignore_index=True)      

        # create ticker and get code
        df['ticker']      = df.apply(lambda x: f'''{x['call_put']}_{asset.replace('.SA','')}_{round(x['strike'],2)}_{x['exp'].strftime('%Y-%b-%d').upper()}''', axis=1)
        df['id'] = df.apply(lambda x: query(x, asset), axis=1)
        df.drop(['strike', 'exp', 'call_put', 'currency', 'ticker'], axis=1, inplace=True)
        
        # make vertical df and format it according to sql table
        df = df.melt(id_vars='id')

        sql = '''
            SELECT field_name AS variable, field_code FROM fields
        '''
        fields = pd.read_sql_query(sql, engine)
        df = df.merge(fields, how='left', on='variable')

        df['rptdt'] = datetime.date.today()#.strftime('%d-%b-%Y')
        df = df[['rptdt', 'id', 'field_code', 'value']]
        df.fillna(0, inplace=True)

        # clear old quotes and insert new ones
        sql = f'''
            DELETE FROM option_quotes
            WHERE rptdt = '{(datetime.date.today()).strftime("%d-%b-%Y")}'
            AND id IN (
                SELECT id FROM options
                WHERE underlying = '{asset}'
            )
        '''
        engine.execute(sql)

        df.to_sql("OPTION_QUOTES".lower(), engine, index=False, if_exists='append')

        count += len(df)

        logger.success(f'finished {asset}, inserted {len(df)} rows')
    
    logger.success(f'inserted {int(count/5)} options for day {(datetime.date.today()).strftime("%d-%b-%Y")}')