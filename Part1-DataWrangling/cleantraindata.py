import pandas as pd
import numpy as np
import datetime

# Read Data Files
transactions = pd.read_csv('transactions_clean.csv')
stores = pd.read_csv('stores.csv', header=0)
transactions = pd.read_csv('transactions_clean.csv', header=0)
items = pd.read_csv('items.csv', header=0)
holidays = pd.read_csv('holidays_events.csv', header=0)
oil = pd.read_csv('oils_clean.csv', header=0)

# Get list of item numbers
itemlist = pd.read_csv('itemlist.csv', header=None)


# Preprocessing
holidays = holidays.rename(columns={'date':'date','type':'holidaytype','locale':'locale','locale_name':'locale_name',
                           'description':'description','transferred':'transferred'})
nationalholidays = holidays[holidays['locale']=='National']
regionalholidays = holidays[holidays['locale']=='Regional']
localholidays = holidays[holidays['locale']=='Local']

localholidays = localholidays.rename(columns={'date':'date','holidaytype':'holidaytype','locale':'locale','locale_name':'city','description':'description','transferred':'transferred'})
regionalholidays = regionalholidays.rename(columns={'date':'date','holidaytype':'holidaytype','locale':'locale','locale_name':'state','description':'description','transferred':'transferred'})


transactions = transactions.melt(id_vars=['date'])
transactions = transactions.rename(columns={'date':'date','variable':'store_nbr','value':'transactions'})
transactions['store_nbr'] = transactions['store_nbr'].map(lambda x: int(x))



def clean(item):
    print(str(item))
    filename= 'items/'+ str(item) + '.csv'
    df = pd.read_csv(filename)
    df['onpromotion'].fillna(False, inplace=True)
    # Combine with other files
    df = df.merge(items, on='item_nbr', how='left')
    df = df.merge(stores, on='store_nbr', how='left')
    #df = df['store_nbr'].map(lambda a: int(a))
    df = df.merge(transactions, how='left', on=['date','store_nbr'])
    df = df.merge(oil, how='left', on='date')
    df = df.merge(nationalholidays, on='date', how='left')
    df = df.merge(regionalholidays, on=['date','state'], how='left', suffixes=('_n','_r'))
    df = df.merge(localholidays, on=['date','city'], how='left')
    # Convert date to datetime
    format = '%Y-%m-%d'
    df['date'] = df['date'].map(lambda a: datetime.datetime.strptime(a, format))
    df['day'] = df['date'].map(lambda x: x.weekday())
    df['month'] = df['date'].map(lambda x: x.month)
    df['year'] = df['date'].map(lambda x: x.year)
    df.to_csv('mergeitems/'+ str(item) + '.csv', index=False)



for index, row in itemlist.iterrows():
    item = int(row[0])
    clean(item)

print('Success!')