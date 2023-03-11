import datetime

import sys
sys.path.insert(1, '//'.join((sys.path[0]).split('\\')[:-1]))

from giraldi_backend import *
from giraldi_futures import *

def fx_conversion(currency, reference_date):
    inverted = ('EUR', 'AUD', 'GBP', 'NZD')
    
    if currency == 'USD':
        return 1
    elif currency in inverted:
        sql = f'''
            SELECT value FROM QUOTES
            WHERE ticker = '{currency}USD'
            AND rptdt = '{reference_date.strftime('%d-%b-%Y')}'
            AND field_code = 1
        '''
        df = pd.read_sql_query(sql, engine)
        if len(df)!=1:
            logger.error(f'''fx_conversion found wrong length df: {len(df)} vs 1 expected for {currency}''')

        return df.iloc[0,0]
    else:
        sql = f'''
            SELECT value FROM QUOTES
            WHERE ticker = 'USD{currency}'
            AND rptdt = '{reference_date.strftime('%d-%b-%Y')}'
            AND field_code = 1
        '''
        df = pd.read_sql_query(sql, engine)
        if len(df)!=1:
            logger.error(f'''fx_conversion found wrong length df: {len(df)} vs 1 expected for {currency}''')

        return 1/df.iloc[0,0]

def get_exposure(quantity, price, ticker, asset_type, currency, fx_px):
    
    if asset_type == 'Cash':
        return 0
    elif asset_type == 'fx':
        if currency == 'USD':
            return -quantity
        else:
            return quantity * fx_px
    elif '_stock' in asset_type or '_etf' in asset_type:
        return quantity * price * fx_px
    elif asset_type == 'fx_future' and ticker.startswith('UC') and currency == 'BRL':
        return -50000 * quantity
    elif asset_type == 'equity_future' and ticker.startswith('IND') and currency == 'BRL':
        return quantity * price * fx_px


if __name__ == '__main__':
    
    rptdt = None
    trader = ''

    if rptdt is None:
        sql = f'''
            SELECT max(rptdt) FROM exposure
            WHERE trader LIKE '%{trader}%'
        '''
        df = pd.read_sql_query(sql, engine)
        if df.empty:
            logger.error(f'No exposure found for trader {trader}')
            sys.exit()
        rptdt = df.iloc[0,0]
    else:
        rptdt = datetime.datetime.strptime('%d-%b-%Y')
    print(rptdt)
    sql = f'''
        SELECT nav FROM navs
        WHERE rptdt = '{rptdt.strftime('%d-%b-%Y')}'
    '''
    nav = pd.read_sql_query(sql, engine)
    if len(nav) != 1:
        logger.error(f'''no NAV found for {rptdt.strftime('%d-%b-%Y')}''')
        sys.exit()
    nav = nav.iloc[0,0]

    sql = f'''
        SELECT trader, asset_type, strategy, ticker, quantity, price, currency FROM exposure
        WHERE rptdt = '{rptdt.strftime('%d-%b-%Y')}'
        AND asset_type <> 'Cash'
    '''
    exposure = pd.read_sql_query(sql, engine)

    exposure['fx_px'] = exposure['currency'].apply(lambda x: fx_conversion(x, rptdt))
    exposure['exp_usd'] = exposure.apply(lambda x: get_exposure(x['quantity'], x['price'], x['ticker'], x['asset_type'], x['currency'], x['fx_px']), axis=1)
    
    missing_exp = exposure.loc[exposure['exp_usd'].isna()]
    if not missing_exp.empty:
        logger.error('missing exposure formulas for the following positions')
        print(missing_exp)
        sys.exit()

    exposure['currency'] = exposure.apply(lambda x: x['ticker'][3:6] if x['ticker'].startswith('USD') and x['asset_type']=='fx' else x['currency'], axis=1)
    exposure['as_nav_%'] = exposure['exp_usd']/nav
    exposure.sort_values(by=['trader', 'strategy', 'asset_type', 'currency', 'as_nav_%'], ascending=[False, True, False, False, False], ignore_index=True, inplace=True)

    fx_exposure = exposure[['trader', 'asset_type','strategy', 'currency', 'exp_usd', 'as_nav_%']].groupby(['trader', 'asset_type', 'strategy', 'currency'], as_index=False).sum()
    fx_exposure = fx_exposure.loc[fx_exposure['currency']!='USD']
    fx_exposure.sort_values(by=['trader', 'asset_type', 'as_nav_%'], ascending=False, inplace=True)

    fx_exposure_agg = fx_exposure[['trader', 'currency', 'exp_usd', 'as_nav_%']].groupby(['trader', 'currency'], as_index=False).sum()
    fx_exposure_agg.sort_values(by=['trader', 'as_nav_%'], ascending=[False, False], inplace=True)
    fx_exposure_agg = pd.concat([fx_exposure_agg, 
        pd.DataFrame([{
            'trader': 'Total: ',
            'currency': 'USD Net',
            'exp_usd': -1*fx_exposure_agg['exp_usd'].sum(),
            'as_nav_%': -1*fx_exposure_agg['as_nav_%'].sum()
        }]),
        pd.DataFrame([{
            'trader': '',
            'currency': 'USD Gross',
            'exp_usd': fx_exposure_agg['exp_usd'].abs().sum(),
            'as_nav_%': fx_exposure_agg['as_nav_%'].abs().sum()
        }])], ignore_index=True)
    
    key = {'us_etf': 0, 'us_stock': 1, 'bz_etf': 2, 'bz_stock': 3}

    equity_exposure = exposure.loc[exposure['asset_type'].isin(['bz_stock', 'us_stock', 'bz_etf', 'us_etf'])].copy()
    equity_exposure['asset_order'] = equity_exposure['asset_type'].apply(lambda x: key[x])
    equity_exposure.sort_values(by=['trader', 'strategy', 'asset_order', 'exp_usd'], ascending=[True, True, True, False], ignore_index=True, inplace=True)
    equity_exposure.drop(['price', 'currency', 'fx_px', 'asset_order'], axis=1, inplace=True)
    equity_exposure = pd.concat([equity_exposure, 
        pd.DataFrame([{
            'ticker': 'Total: ',
            'quantity': 'USD Net',
            'exp_usd': equity_exposure['exp_usd'].sum(),
            'as_nav_%': equity_exposure['as_nav_%'].sum()
        }]),
        pd.DataFrame([{
            'ticker': '',
            'quantity': 'USD Gross',
            'exp_usd': equity_exposure['exp_usd'].abs().sum(),
            'as_nav_%': equity_exposure['as_nav_%'].abs().sum()
        }])], ignore_index=True)
    

    with pd.ExcelWriter(r'C:/Projects/Macroboys Capital/reports/exposure_output.xlsx') as writer:
        exposure.to_excel(writer, sheet_name='exposure', index=False)
        fx_exposure.to_excel(writer, sheet_name='fx_exposure', index=False)
        fx_exposure_agg.to_excel(writer, sheet_name='fx_exposure_agg', index=False)
        equity_exposure.to_excel(writer, sheet_name='equity_exposure', index=False)