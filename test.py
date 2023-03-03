from giraldi_backend import *

df = pd.read_excel('holidays.xlsx')
df['REGION'] = 'brazil'
print(df)

df.to_sql('holidays', engine, index=False, if_exists='append')