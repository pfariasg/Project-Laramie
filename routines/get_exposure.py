import sys
sys.path.insert(1, '//'.join((sys.path[0]).split('\\')[:-1]))

import datetime
from giraldi_backend import *

dull = True

def net_cash(df):
    df['position'] = df['quantity'] * df['price']
    df['position'] = df.apply(lambda x: 0 if x['asset_type'][-6:]=='future' else x['position'], axis=1)

    total = df['position'].sum()
    df.drop('position', axis=1, inplace=True) 
    return total


# get date of last exposure
sql = f'''
    SELECT max(rptdt) FROM exposure
'''
date = pd.read_sql_query(sql, engine).iloc[0,0].date()
today = datetime.date.today() #date(2023,2,17) #

while date != today:
    date += datetime.timedelta(days=1)
    if date.weekday() >= 5:
        continue
    # elif input(f'skip {date.strftime("%d-%b-%Y")}? ') == 'y':
    #     continue
    
    logger.info(f'Working on exposure for {date.strftime("%d-%b-%Y")}')

    sql = f'''
        SELECT rptdt, strategy, fund, book, trader, ticker, asset_type, quantity, price, currency, obs FROM exposure
        WHERE rptdt = (
            SELECT max(rptdt) FROM exposure
        )
    '''
    exposure = pd.read_sql_query(sql, engine)

    sql = f'''
        SELECT rptdt, strategy, fund, book, trader, ticker, asset_type, quantity, price, currency, obs FROM trades
        WHERE rptdt = '{date.strftime("%d-%b-%Y")}'
    '''
    trades = pd.read_sql_query(sql, engine)
        
    if trades.empty:
# FIX OLD PRICES
        logger.info(f'No trades for {date.strftime("%d-%b-%Y")}')
        exposure['rptdt'] = date
        if not dull:
            exposure.to_sql('exposure', engine, index=False, if_exists='append')
    
    else:
        logger.info(f'Found {len(trades)} trades for {date.strftime("%d-%b-%Y")}')

        # producing cash impact
        yest_cash = net_cash(trades)
        
        exposure = pd.concat([exposure, trades])

        exposure['quantity'] = exposure.apply(lambda x: (x['quantity']-yest_cash) if x['asset_type']=='Cash' and x['ticker']=='Cash' else x['quantity'], axis=1)
        del yest_cash
        
        

        #building today's exposure
        exposure['rptdt'] = date
        exposure.to_clipboard()
        sys.exit()
        exposure['long'] = exposure.apply(lambda x: x['quantity']/abs(x['quantity']), axis=1)
        

        exposure = exposure.groupby(['rptdt', 'strategy', 'fund', 'book', 'trader', 'ticker', 'asset_type', 'currency', 'obs', 'long'], as_index=False, dropna=False).apply(
            lambda x: pd.Series([np.sum(x['quantity']), np.average(x['price'], weights=x['quantity'])], index=['quantity', 'price']))
        exposure = exposure[['rptdt', 'strategy', 'fund', 'book', 'trader', 'ticker', 'asset_type', 'quantity', 'price', 'currency', 'obs']]
        
        # get current prices
        stocks = exposure.loc[exposure['asset_type'].str.contains('stock'), 'ticker'].unique()
        sql = f'''
            SELECT ticker, value FROM stock_quotes
            WHERE rptdt = '{date.strftime("%d-%b-%Y")}'
            AND ticker IN ('{"','".join(stocks)}')
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

        quotes = pd.concat([options, stocks, futures, pd.DataFrame({'ticker': ['Cash'], 'value': [1]})])
        
        del stocks, options, futures

        # update prices on the exposure
        exposure = exposure.merge(quotes, how='left')

        missing_prices = exposure[exposure['value'].isna()]
        if not missing_prices.empty:
            logger.error(f'missing {date.strftime("%d-%b-%Y")} quotes for assets')
            print(missing_prices[['trader', 'asset_type', 'ticker']])
            sys.exit()
        
        if len(exposure.loc[exposure['asset_type'].str.contains('future'), 'ticker']):
            exposure.to_clipboard()
            # margin_impact = get_margin_impact(exposure)
        
        
        sys.exit()
        # exposure.drop('price', axis=1, inplace=True)
        exposure.rename({'value': 'price'}, axis=1, inplace=True)

        exposure.fillna(.16, inplace=True) # del later
        # today_nav = net_cash(exposure)
        # print(today_nav)

        # insert exposure
        sql = f'''
            DELETE FROM exposure 
            WHERE rptdt = '{date.strftime("%d-%b-%Y")}'
        '''
        if not dull:
            engine.execute(sql)
            exposure.to_sql('exposure', engine, index=False, if_exists='append')
        print(exposure)

