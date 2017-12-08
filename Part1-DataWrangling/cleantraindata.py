import pandas as pd
import numpy as np
import datetime

# Read Data Files
stores = pd.read_csv('../data/stores.csv', header=0)
transactions = pd.read_csv('../data/transactions_clean.csv', header=0)
items = pd.read_csv('../data/items.csv', header=0)
holidays = pd.read_csv('../data/holidays_events.csv', header=0)
oil = pd.read_csv('../data/oils_clean.csv', header=0)

# Get list of item numbers
itemlist = pd.read_csv('itemlist.csv', header=None)
#itemlist = pd.read_csv('itemlist4.csv', header=None)

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

# Functions
def promo(x):
    if (x):
        return 1
    else:
        return 0

def holiday(x, y, z):
    if (x =='Holiday' or y=='Holiday' or z=='Holiday'):
        return 1
    else:
        return 0

def holiday2(x, y, z):
    if (x !='None'):
        return 'National ' + x
    elif (y !='None'):
        return 'Regional ' + y
    elif(z !='None'):
        return 'Local ' + z
    else:
        return 'None'

def trans(x, y, z):
    if (x==True or y==True or z==True):
        return 1
    else:
        return 0

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
    return df

def featureengineer(unitsales):
    print('engineering features')
    unitsales['onpromotion'] = unitsales['onpromotion'].map(lambda a: promo(a))
    family = pd.get_dummies(unitsales['family'])
    unitsales = unitsales.join(family)
    unitsales.drop('family', axis=1, inplace=True)
    
    city = pd.get_dummies(unitsales['city'])
    unitsales = unitsales.join(city)
    unitsales.drop('city', axis=1, inplace=True)
    
    
    state = pd.get_dummies(unitsales['state'], prefix='state_')
    unitsales = unitsales.join(state)
    unitsales.drop('state', axis=1, inplace=True)
    
    
    type = pd.get_dummies(unitsales['type'], prefix='store_')
    unitsales = unitsales.join(type)
    unitsales.drop('type', axis=1, inplace=True)

    cluster = pd.get_dummies(unitsales['cluster'], prefix='store_')
    unitsales = unitsales.join(cluster)
    unitsales.drop('cluster', axis=1, inplace=True)

    unitsales['holidaytype'] = unitsales['holidaytype'].fillna('None')
    unitsales['holidaytype_r'] = unitsales['holidaytype_r'].fillna('None')
    unitsales['holidaytype_n'] = unitsales['holidaytype_n'].fillna('None')
    unitsales['holiday'] = unitsales.apply(lambda x: holiday(x['holidaytype_n'], x['holidaytype_r'], x['holidaytype']), axis=1)
    unitsales['holiday2'] = unitsales.apply(lambda x: holiday2(x['holidaytype_n'], x['holidaytype_r'], x['holidaytype']), axis=1)
    holidayevents = pd.get_dummies(unitsales['holiday2'])
    unitsales = unitsales.join(holidayevents)
    unitsales.drop('holiday2', axis=1, inplace=True)

    d1 = pd.get_dummies(unitsales['description'])
    unitsales = unitsales.join(d1)
    unitsales.drop('description', axis=1, inplace=True)

    d2 = pd.get_dummies(unitsales['description_n'])
    unitsales = unitsales.join(d2)
    unitsales.drop('description_n', axis=1, inplace=True)

    d3 = pd.get_dummies(unitsales['description_r'])
    unitsales = unitsales.join(d3)
    unitsales.drop('description_r', axis=1, inplace=True)

    unitsales['net_tranferred'] = unitsales.apply(lambda x: trans(x['transferred_n'], x['transferred_r'], x['transferred']), axis=1)
    
    # Drop unnecessary features
    untisales.drop('id', axis=1, inplace=True)
    unitsales.drop('date', axis=1, inplace=True)
    unitsales.drop('transferred', axis=1, inplace=True)
    unitsales.drop('transferred_n', axis=1, inplace=True)
    unitsales.drop('transferred_r', axis=1, inplace=True)
    unitsales.drop('locale_n', axis=1, inplace=True)
    unitsales.drop('locale_r', axis=1, inplace=True)
    unitsales.drop('locale', axis=1, inplace=True)
    unitsales.drop('locale_name', axis=1, inplace=True)
    unitsales.drop('holidaytype', axis=1, inplace=True)
    unitsales.drop('holidaytype_n', axis=1, inplace=True)
    unitsales.drop('holidaytype_r', axis=1, inplace=True)
    
    # Remove outliers
    unitsales = unitsales[unitsales['unit_sales']<1000]
    unitsales = unitsales[unitsales['unit_sales']>-1000]

    return unitsales



# Process files
for index, row in itemlist.iterrows():
    item = int(row[0])
    df = clean(item)
    df = featureengineer(df)
    df.to_csv('mergeitems/'+ str(item) + '.csv', index=False)


print('Success!')