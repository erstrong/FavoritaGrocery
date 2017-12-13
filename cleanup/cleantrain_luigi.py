import pandas as pd
import numpy as np
import datetime

# Read Data Files
unitsales = pd.read_csv('../data/train.csv', header=0, low_memory=False)
stores = pd.read_csv('../data/stores.csv', header=0)
transactions = pd.read_csv('../data/transactions_clean.csv', header=0)
items = pd.read_csv('../data/items.csv', header=0)
holidays = pd.read_csv('../data/holidays_events.csv', header=0)
oil = pd.read_csv('../data/oils_clean.csv', header=0)



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

# Clean the training data
unitsales.drop('id', axis=1, inplace=True)
unitsales['onpromotion'].fillna(False, inplace=True)

# Remove outliers
unitsales = unitsales[unitsales['unit_sales']<1000]
unitsales = unitsales[unitsales['unit_sales']>-1000]

# Combine with other files
unitsales = unitsales.merge(items, on='item_nbr', how='left')
unitsales = unitsales.merge(stores, on='store_nbr', how='left')
unitsales['store_nbr'] = unitsales['store_nbr'].map(lambda a: int(a))
unitsales = unitsales.merge(transactions, how='left', on=['date','store_nbr'])
unitsales = unitsales.merge(oil, how='left', on='date')
unitsales = unitsales.merge(nationalholidays, on='date', how='left')
unitsales = unitsales.merge(regionalholidays, on=['date','state'], how='left', suffixes=('_n','_r'))
unitsales = unitsales.merge(localholidays, on=['date','city'], how='left')

# Convert date to datetime
format = '%Y-%m-%d'
unitsales['date'] = unitsales['date'].map(lambda a: datetime.datetime.strptime(a, format))
unitsales['day'] = unitsales['date'].map(lambda x: x.weekday())
unitsales['month'] = unitsales['date'].map(lambda x: x.month)
unitsales['year'] = unitsales['date'].map(lambda x: x.year)

# Write file
unitsales.to_csv('train_finalclean.csv', index=False)


