import datetime
import numpy as np
from giraldi_backend import *
import holidays as hol

def get_di_bdays(ticker, reference_date):
    exp = ticker[-3:]
    futures_convention = {
        'F': 1, 'G': 2, 'H': 3, 'J': 4, 'K': 5, 'M': 6,
        'N': 7, 'Q': 8, 'U': 9, 'V': 10, 'X': 11, 'Z': 12
    }
    expiration = datetime.date(day=1,month=futures_convention[exp[0]], year=2000+int(ticker[-2:]))
    while expiration.weekday() >= 5:
        expiration += datetime.timedelta(days=1)
    
    holidays = hol.CountryHoliday('BR')

    print(np.busday_count(reference_date.date(), expiration, holidays=holidays[reference_date:expiration]))
    sys.exit()

def di_settlement(ticker, quantity, price, value, date, reference_date):
    get_di_bdays(ticker, reference_date)
    
    if reference_date == date:
        print(f'traded today: {quantity} {price}')
    elif reference_date > date:
        print(f'traded yesterday: {quantity} {price}')

def get_settlement(row, reference_date):

    if row['ticker'].startswith('OD') and row['asset_type'] == 'br_future' and row['currency'] == 'USD':
        # print(row)
        return di_settlement(row['ticker'],row['quantity'],row['price'], row['value'], row['rptdt'], reference_date)


def get_margin_impact(df):
    reference_date = df['rptdt'].max()

    df = df[df['asset_type'].str.contains('future')].copy()



    df['settlement'] = df.apply(lambda x: get_settlement(x, reference_date), axis=1)

    # df.append()
    print(df)

    # transformar settlement em impacto no caixa


    
if __name__ == '__main__':
    dfl = pd.read_excel('test.xlsx')
    get_margin_impact(dfl)
