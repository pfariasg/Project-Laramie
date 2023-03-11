import sys
sys.path.insert(1, '//'.join((sys.path[0]).split('\\')[:-1]))

import datetime
import matplotlib.pyplot as plt

from giraldi_backend import *
from giraldi_futures import *

if __name__ == '__main__':

    trader = ''
    fund = ''

    asset_type = ''
    ticker = ''

    rptdt_st = '06-Mar-2023'
    rptdt_end = ''



    sql = 'SELECT max(rptdt) FROM results'
    last_dt = pd.read_sql_query(sql, engine)
    if len(last_dt) != 1:
        logger.error(f'found wrong number of max dates: {len(last_dt)} instead of 1')
        sys.exit()

    if rptdt_st == '':
        rptdt_st = last_dt.iloc[0,0]
    else:
        rptdt_st = datetime.datetime.strptime(rptdt_st, '%d-%b-%Y')
    
    if rptdt_end == '':
        rptdt_end = last_dt.iloc[0,0]
    else:
        rptdt_end = datetime.datetime.strptime(rptdt_end, '%d-%b-%Y')

    sql = f'''
        SELECT nav FROM navs
        WHERE rptdt = (
            SELECT max(rptdt) FROM navs
            WHERE rptdt < '{rptdt_st.strftime('%d-%b-%Y')}'
        )
        AND fund LIKE '%{fund}%'
    '''
    yest_nav = pd.read_sql_query(sql, engine)
    if len(yest_nav) != 1:
        logger.error(f'found wrong number of max dates: {len(yest_nav)} instead of 1')
        sys.exit()
    yest_nav = yest_nav.iloc[0,0]
    
    sql = f'''
        SELECT strategy, fund, book, trader, ticker, asset_type, currency, result AS result_usd FROM results
        WHERE rptdt >= '{rptdt_st.strftime('%d-%b-%Y')}'
        AND rptdt <= '{rptdt_end.strftime('%d-%b-%Y')}'
        AND trader LIKE '%{trader}%'
        AND fund LIKE '%{fund}%'
        AND asset_type LIKE '%{asset_type}%'
        AND ticker LIKE '%{ticker}%'
    '''
    results = pd.read_sql_query(sql, engine)

    results = results.groupby(['strategy', 'fund', 'book', 'trader', 'ticker', 'asset_type', 'currency'], as_index=False).sum()
    
    results.sort_values(['trader', 'strategy', 'asset_type', 'currency', 'result_usd'], ascending=[False, True, False, False, False], ignore_index=True, inplace=True)
    results.drop(['fund', 'book'], axis=1, inplace=True)
    results['contribution'] = results['result_usd']/yest_nav

    results_strategy = results[['trader', 'strategy', 'asset_type', 'currency', 'result_usd']]
    results_strategy = results_strategy.groupby(['trader', 'strategy', 'asset_type', 'currency'], as_index=False).sum()
    results_strategy.sort_values(['trader', 'strategy', 'asset_type', 'currency', 'result_usd'], ascending=[False, True, False, False, False], ignore_index=True, inplace=True)
    results_strategy['contribution'] = results_strategy['result_usd']/yest_nav
    # print(results)
    # print(results_strategy)

    with pd.ExcelWriter(r'C:/Projects/Macroboys Capital/reports/attribution_output.xlsx') as writer:
        results.to_excel(writer, sheet_name='results', index=False)
        results_strategy.to_excel(writer, sheet_name='results_strategy', index=False)

    sql = f'''
        SELECT rptdt, nav FROM navs
        WHERE fund='Laramie'
        ORDER BY rptdt
    '''
    perf = pd.read_sql_query(sql, engine)
    perf['rptdt'] = pd.to_datetime(perf['rptdt']).dt.strftime('%d-%b-%Y')
    perf['nav'] = perf['nav']/perf['nav'].iloc[0]*100
    
    fig, ax = plt.subplots()
    ax.plot(perf['rptdt'], perf['nav'], c='midnightblue')
    
    fig.set_size_inches(9,3)
    plt.savefig(r'C:/Projects/Macroboys Capital/reports/performance.png', dpi=500, bbox_inches='tight')