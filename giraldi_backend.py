import pandas as pd
pd.set_option('display.float_format', lambda x: '%.3f' % x)

import sys
from loguru import logger
import sqlalchemy

try:
    engine = sqlalchemy.create_engine('oracle+cx_oracle://system:noperope@192.168.0.10:1521/xe')
except Exception:
    logger.error(f'Unable to create engine: {Exception}')
    sys.exit()


def get_asset_type(ticker):
    sql = f'''
        SELECT asset_type FROM tickers 
        WHERE ticker = '{ticker}'
    '''
    asset_type = pd.read_sql_query(sql, engine)

    if asset_type.empty:
        logger.warning(f'Asset {ticker} not in TICKERS. Creating new ticker')
        asset_type = input(f'Asset type: ').lower()

        sql = f'''
            SELECT DISTINCT asset_type FROM tickers
            WHERE asset_type = '{asset_type}'
        '''
        if pd.read_sql_query(sql, engine).empty():
            logger.error(f'Invalid asset type')
            sys.exit()

        name = input(f'Name: ')

        sql = f'''
            INSERT INTO tickers (ticker, asset_type, name)
            VALUES ('{ticker}', '{asset_type}', '{name}')
        '''
        engine.execute(sql)
        logger.warning(f'Created new ticker {ticker}')
    else:
        asset_type = asset_type.iloc[0,0]

    return asset_type
    

def get_trade_id(trade_id):
    # get new trade id if new trade
    if trade_id == None:
        sql = '''
            SELECT max(trade_id) FROM trades
        '''
        trade_id = pd.read_sql_query(sql, engine).iloc[0,0] + 1

    return trade_id


def weighted_average(df,data_col,weight_col,by_col):
    df['_data_times_weight'] = df[data_col]*df[weight_col]
    df['_weight_where_notnull'] = df[weight_col]*pd.notnull(df[data_col])
    g = df.groupby(by_col)
    result = g['_data_times_weight'].sum() / g['_weight_where_notnull'].sum()
    del df['_data_times_weight'], df['_weight_where_notnull']
    return result