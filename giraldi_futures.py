from giraldi_backend import *

# def get_di_bdays(ticker, reference_date):
#     exp = ticker[-3:]
#     futures_conversion = {
#         'F': 1, 'G': 2, 'H': 3, 'J': 4, 'K': 5, 'M': 6,
#         'N': 7, 'Q': 8, 'U': 9, 'V': 10, 'X': 11, 'Z': 12
#     }
#     expiration = datetime.date(day=1,month=futures_conversion[exp[0]], year=2000+int(ticker[-2:]))
#     while expiration.weekday() >= 5:
#         expiration += datetime.timedelta(days=1)
    
#     holidays = hol.CountryHoliday('BR')

#     return np.busday_count(reference_date.date(), expiration, holidays=hol.CountryHoliday('US')[reference_date:expiration])
def fx_settl_conversion(row, reference_date):
    inverted = ('EUR', 'AUD', 'GBP', 'NZD')
    
    if row['currency'] == 'USD':
        return row['settlement']
    elif row['currency'] in inverted:
        sql = f'''
            SELECT value FROM QUOTES
            WHERE ticker = '{row['currency']}USD'
            AND rptdt = '{reference_date.strftime('%d-%b-%Y')}'
            AND field_code = 1
        '''
        df = pd.read_sql_query(sql, engine)
        if len(df)!=1:
            logger.error(f'''fx_settl_conversion found wrong length df: {len(df)} vs 1 expected for {row['currency']}''')

        return row['settlement']*df.iloc[0,0]
    else:
        sql = f'''
            SELECT value FROM QUOTES
            WHERE ticker = 'USD{row['currency']}'
            AND rptdt = '{reference_date.strftime('%d-%b-%Y')}'
            AND field_code = 1
        '''
        df = pd.read_sql_query(sql, engine)
        if len(df)!=1:
            logger.error(f'''fx_settl_conversion found wrong length df: {len(df)} vs 1 expected for {row['currency']}''')

        return row['settlement']/df.iloc[0,0]


def di_face_value(i, n):
    return 100000/pow(1+i/100,n/252)


def di_settlement(ticker, quantity, price, value, date, reference_date):
    sql = f'''
        SELECT du FROM term_structure
        WHERE ticker = '{ticker}'
        AND rptdt = '{reference_date.strftime('%d-%b-%Y')}'
    '''
    du = pd.read_sql_query(sql, engine)
    if du.empty:
        logger.error(f'''no data found for {ticker} at {reference_date.strftime('%d-%b-%Y')} on table term_structure''')
        sys.exit()

    du = du.iloc[0,0]
    
    p_t = di_face_value(value, du)

    if reference_date == date:
        # print(f'traded today: {quantity} {price}')
        po = di_face_value(price, du)

        return (p_t-po) * -quantity

    elif reference_date > date:
        # print(f'traded yesterday: {quantity} {price}')

        sql = f'''
            SELECT value FROM term_structure
            WHERE ticker = 'CDI'
            AND rptdt = '{reference_date.strftime('%d-%b-%Y')}'
        '''
        cdi = pd.read_sql_query(sql, engine)
        if cdi.empty:
            logger.error(f'''CDI for {reference_date.strftime('%d-%b-%Y')} not inserted in term_structure''')
        cdi = cdi['value'].values[0]

        p_tm1 = di_face_value(price, du)

        return (p_t-p_tm1*pow(1+cdi/100,1/252)) * -quantity
    
def ibov_fut_settlement(quantity, price, value):
    return quantity * (value-price)

def dol_fut_settlement(quantity, price, value):
    return 50000 * quantity * (value-price)

def nasdaq_100_settlement(quantity, price, value):
    return 20 * quantity * (value-price)

def _100_m_rate_settlement(quantity, price, value, contract_unit):
    return quantity * (value-price) * contract_unit

def bond_futures_settlement(quantity, price, value):
    return quantity * 1000 * (value-price)

def get_settlement(row, reference_date):

    if row['ticker'].startswith('OD') and row['asset_type'] == 'br_future' and row['currency'] == 'BRL':
        return di_settlement(row['ticker'],row['quantity'],row['price'], row['value'], row['rptdt'], reference_date)
    
    elif row['ticker'].startswith('IND') and row['asset_type'] == 'equity_future' and row['currency'] == 'BRL':
        return ibov_fut_settlement(row['quantity'],row['price'], row['value'])
    
    elif row['ticker'].startswith('UC') and row['asset_type'] == 'fx_future' and row['currency'] == 'BRL':
        return dol_fut_settlement(row['quantity'],row['price'], row['value'])
    
    elif row['ticker'].startswith('NQ') and row['asset_type'] == 'equity_future' and row['currency'] == 'USD':
        # https://www.cmegroup.com/markets/equities/nasdaq/e-mini-nasdaq-100.quotes.html
        return nasdaq_100_settlement(row['quantity'],row['price'], row['value'])

    elif row['ticker'].startswith(('ZQ', 'SR1')) and row['asset_type'] == 'ir_future' and row['currency'] == 'USD':
        # https://www.cmegroup.com/markets/interest-rates/stirs/30-day-federal-fund.contractSpecs.html
        # 30 Days Fed Funds
        # https://www.cmegroup.com/markets/interest-rates/stirs/one-month-sofr.html
        # 1-Month SOFR
        return _100_m_rate_settlement(row['quantity'], row['price'], row['value'], contract_unit=4167)
    
    elif row['ticker'].startswith(('ZQ', 'SR3')) and row['asset_type'] == 'ir_future' and row['currency'] == 'USD':
        # https://www.cmegroup.com/markets/interest-rates/stirs/three-month-sofr.contractSpecs.html
        # 3-Month SOFR
        return _100_m_rate_settlement(row['quantity'], row['price'], row['value'], contract_unit=2500)
    
    elif row['ticker'].startswith(('ZF', 'ZN', 'TN', 'UB')) and row['asset_type'] == 'bond_future' and row['currency'] == 'USD':
        # https://www.cmegroup.com/markets/interest-rates/us-treasury/5-year-us-treasury-note.contractSpecs.html
        # 5-Year T-Note futures
        # https://www.cmegroup.com/markets/interest-rates/us-treasury/10-year-us-treasury-note.html
        # 10-Year T-Note futures
        # https://www.cmegroup.com/markets/interest-rates/us-treasury/30-year-us-treasury-bond.html
        # 30-Year T-Note Futures
        # https://www.cmegroup.com/markets/interest-rates/us-treasury/ultra-10-year-us-treasury-note.contractSpecs.html
        # Ultra 10-Year T-Note Futures
        # https://www.cmegroup.com/markets/interest-rates/us-treasury/ultra-t-bond.contractSpecs.html
        # Ultra 30-Year T-Note Futures

        return bond_futures_settlement(row['quantity'], row['price'], row['value'])

    elif row['asset_type'] == 'fx':
        return row['quantity']*-(row['price']/row['value']-1)
    



def get_margin_impact(df):
    reference_date = df['rptdt'].max() #date of the exposure that's being calculated

    df = df[df['asset_type'].str.contains('_future|fx', regex=True)].copy()
    
    df['settlement'] = df.apply(lambda x: get_settlement(x, reference_date), axis=1)
    df['settlement'] = df.apply(lambda x: fx_settl_conversion(x, reference_date), axis=1)
    
    missing_settl = df.loc[df['settlement'].isna()]
    if not missing_settl.empty:
        logger.error('missing settlement formulas for the following positions')
        print(missing_settl)
        sys.exit()

    df.rename({'settlement': 'performance'}, axis=1, inplace=True)
    
    return df, df['performance'].sum()

    
if __name__ == '__main__':
    dfl = pd.read_excel('test.xlsx')
    print(dfl)
    get_margin_impact(dfl)
