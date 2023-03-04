import sys
sys.path.insert(1, '//'.join((sys.path[0]).split('\\')[:-1]))

import datetime
import numpy as np
from giraldi_backend import *
from giraldi_futures import *

dull = True

def fx_conversion(currency, position, reference_date):
    inverted = ('EUR', 'AUD', 'GBP', 'NZD')
    
    if currency == 'USD':
        return position
    elif currency in inverted:
        sql = f'''
            SELECT value FROM QUOTES
            WHERE ticker = '{currency}USD'
            AND rptdt = '{reference_date.strftime('%d-%b-%Y')}'
            AND field_code = 1
        '''
        df = pd.read_sql_query(sql, engine)
        if len(df)!=1:
            logger.error(f'''fx_settl_conversion found wrong length df: {len(df)} vs 1 expected for {currency}''')

        return position*df.iloc[0,0]
    else:
        sql = f'''
            SELECT value FROM QUOTES
            WHERE ticker = 'USD{currency}'
            AND rptdt = '{reference_date.strftime('%d-%b-%Y')}'
            AND field_code = 1
        '''
        df = pd.read_sql_query(sql, engine)
        if len(df)!=1:
            logger.error(f'''fx_settl_conversion found wrong length df: {len(df)} vs 1 expected for {currency}''')

        return position/df.iloc[0,0]

def net_cash(df, date):
    df['position'] = df['quantity'] * df['price']
    df['position'] = df.apply(lambda x: 0 if x['asset_type'][-6:]=='future' or x['asset_type']=='fx' else x['position'], axis=1)
    df['position'] = df.apply(lambda x: fx_conversion(x['currency'], x['position'], date), axis=1)
    
    total = df['position'].sum()
    
    df.drop('position', axis=1, inplace=True)
    return total

def get_quotes(exposure, date):
    # get current prices
    stocks = exposure.loc[exposure['asset_type'].str.contains('_stock', regex=False), 'ticker'].unique()
    fx     = exposure.loc[exposure['asset_type'].str.contains(    'fx', regex=False), 'ticker'].unique()
    etfs     = exposure.loc[exposure['asset_type'].str.contains('_etf', regex=False), 'ticker'].unique()
    # tickers = stocks + fx
    sql = f'''
        SELECT ticker, value FROM quotes
        WHERE rptdt = '{date.strftime("%d-%b-%Y")}'
        AND ticker IN ('{"','".join(np.concatenate((stocks, fx, etfs)))}')
        AND field_code = 1
    '''
    stocks = pd.read_sql_query(sql, engine)    

    options = exposure.loc[exposure.asset_type=='option', 'ticker'].unique()
    sql = f'''
        SELECT ticker, value FROM option_quotes oq
            LEFT JOIN (
                SELECT unique id, ticker FROM options) op
            ON oq.id = op.id
            WHERE ticker IN ('{"','".join(options)}')
            AND oq.field_code = 1
            AND rptdt = '{date.strftime("%d-%b-%Y")}'
        '''
    options = pd.read_sql_query(sql, engine)

    futures = exposure.loc[exposure['asset_type'].str.contains('future'), 'ticker'].unique()
    sql = f'''
        SELECT ticker, value FROM term_structure
            WHERE ticker in ('{"','".join(futures)}')
            AND rptdt = '{date.strftime("%d-%b-%Y")}'
        '''
    futures = pd.read_sql_query(sql, engine)
    
    return pd.concat([options, stocks, futures, pd.DataFrame({'ticker': ['Cash'], 'value': [1]})])

if __name__ == '__main__':
    # get date of last exposure
    sql = f'''
        SELECT max(rptdt) FROM exposure
    '''
    date = pd.read_sql_query(sql, engine).iloc[0,0].date()
    today = datetime.date.today()

    # add a for loop on fund name to adapt to multiple funds
    while date != today:
        for fund in ['Laramie']:
            date += datetime.timedelta(days=1)
            if date.weekday() >= 5:
                continue
            # elif input(f'skip {date.strftime("%d-%b-%Y")}? ') == 'y':
            #     continue

            logger.info(f'Working on exposure for {date.strftime("%d-%b-%Y")}')

            sql = f'''
                SELECT rptdt, strategy, fund, book, trader, ticker, asset_type, quantity, price, currency FROM exposure
                WHERE rptdt = (
                    SELECT max(rptdt) FROM exposure
                )
                AND fund = '{fund}'
            '''
            exposure = pd.read_sql_query(sql, engine)

            sql = f'''
                SELECT rptdt, strategy, fund, book, trader, ticker, asset_type, quantity, price, currency FROM trades
                WHERE rptdt = '{date.strftime("%d-%b-%Y")}'
                AND fund = '{fund}'
            '''
            trades = pd.read_sql_query(sql, engine)

            if trades.empty:
                logger.info(f'No trades for {date.strftime("%d-%b-%Y")}')

            if not trades.empty:
                logger.info(f'Found {len(trades)} trades for {date.strftime("%d-%b-%Y")}')

                # producing cash impact
                yest_cash = net_cash(trades, date)

                exposure = pd.concat([exposure, trades])

                exposure['quantity'] = exposure.apply(lambda x: (x['quantity']-yest_cash) if x['asset_type']=='Cash' and x['ticker']=='Cash' else x['quantity'], axis=1)

                del yest_cash

            quotes = get_quotes(exposure, date)

            # update prices on the exposure
            exposure = exposure.merge(quotes, how='left')

            missing_prices = exposure[exposure['value'].isna()]
            if not missing_prices.empty:
                logger.error(f'missing {date.strftime("%d-%b-%Y")} quotes for assets')
                print(missing_prices[['trader', 'asset_type', 'ticker']])
                sys.exit()

            if len(exposure.loc[exposure['asset_type'].str.contains('future'), 'ticker']):
                margin_impact = get_margin_impact(exposure)
                # print(exposure)
                # print('\n', margin_impact)
                exposure['quantity'] = exposure.apply(lambda x: x['quantity'] + margin_impact if x['ticker'] == 'Cash' else x['quantity'], axis=1)
                # print(exposure)

            exposure.drop('price', axis=1, inplace=True)
            exposure.rename({'value': 'price'}, axis=1, inplace=True)

            cols = exposure.columns
            exposure = exposure.groupby([column for column in cols if column != 'quantity'], as_index=False).sum()
            exposure = exposure[cols]

            exposure['rptdt'] = date        
            # today_nav = net_cash(exposure)

            # insert exposure
            sql = f'''
                DELETE FROM exposure 
                WHERE rptdt = '{date.strftime("%d-%b-%Y")}'
            '''
            if not dull:
                engine.execute(sql)
                exposure.to_sql('exposure', engine, index=False, if_exists='append')
            # print(exposure)
            # exposure.to_clipboard()
            # sys.exit()

