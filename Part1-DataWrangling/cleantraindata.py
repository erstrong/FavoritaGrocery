import pandas as pd
import numpy as np
import datetime

# Read Data Files
stores = pd.read_csv('stores.csv', header=0)
transactions = pd.read_csv('transactions_clean.csv', header=0)
items = pd.read_csv('items.csv', header=0)
holidays = pd.read_csv('holidays_events.csv', header=0)
oil = pd.read_csv('oils_clean.csv', header=0)

# Get list of item numbers
itemlist = pd.read_csv('itemlist.csv', header=None)


# Preprocessing
# stores
stores.drop('type', axis=1, inplace=True)
stores.drop('cluster', axis=1, inplace=True)

# holidays
holidays = holidays.rename(columns={'date':'date','type':'holidaytype','locale':'locale','locale_name':'locale_name',
                           'description':'description','transferred':'transferred'})

holidays.drop('transferred', axis=1, inplace=True)

nationalholidays = holidays[holidays['locale']=='National'].drop('locale', axis=1).drop('locale_name', axis=1)

regionalholidays = holidays[holidays['locale']=='Regional'].drop('locale', axis=1).drop('description', axis=1)
localholidays = holidays[holidays['locale']=='Local'].drop('locale', axis=1).drop('description', axis=1)

nationalholidays = nationalholidays.rename(columns={'date':'date','type':'holidaytype','description_n':'description'})
localholidays = localholidays.rename(columns={'date':'date','holidaytype':'holidaytype','locale_name':'city'})
regionalholidays = regionalholidays.rename(columns={'date':'date','holidaytype':'holidaytype','locale_name':'state'})

# transactions
transactions = transactions.melt(id_vars=['date'])
transactions = transactions.rename(columns={'date':'date','variable':'store_nbr','value':'transactions'})
transactions['store_nbr'] = transactions['store_nbr'].map(lambda x: int(x))

def promo(x):
    if (x==True):
        return 1
    else:
        return 0

def holidayflag(x):
    if (x =='Holiday'):
        return 1
    else:
        return 0

def earthquakedrop(x):
    list=['Terremoto Manabi+9','Terremoto Manabi+10','Terremoto Manabi+11','Terremoto Manabi+12','Terremoto Manabi+13']
    if (x.isin(list)):
        return 1
    else:
        return 0

def nationalother(x):
    list = ['Additional','Bridge','Transfer','Work Day']
    if (x.isin(list)):
        return 1
    else:
        return 0

def nholidayspike(x):
    list=['Dia de Difuntos','Dia del Trabajo','Independencia de Cuenca','Primer dia del ano']
    if (x.isin(list)):
        return 1
    else:
        return 0

def goodfriday(x):
    if (x == 'Viernes Santo'):
        return 1
    else:
        return 0

def blackfriday(x):
    if (x == 'Black Friday'):
        return 1
    else:
        return 0

def worldcupspike(x):
    if (x.str.contains('Final')):
        return 1
    elif(x.str.contains('Tercer')):
        return 1
    elif(x.str.contains('Ecuador-Suiza')):
        return 1
    else:
        return 0

def clean(item):
    #print(str(item))
    filename= 'items/'+ str(item) + '.csv'
    unitsales = pd.read_csv(filename)
    unitsales['onpromotion'].fillna(False, inplace=True)
    unitsales['onpromotion'] = unitsales['onpromotion'].map(lambda a: promo(a))
    # Remove outliers
    unitsales = unitsales[unitsales['unit_sales']<1000]
    unitsales = unitsales[unitsales['unit_sales']>-1000]
    
    
    # Combine with other files
    unitsales = unitsales.merge(items, on='item_nbr', how='left')
    unitsales = unitsales.merge(stores, on='store_nbr', how='left')
    #unitsales = unitsales['store_nbr'].map(lambda a: int(a))
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

    day = pd.get_dummies(unitsales['day'],prefix='day')
    unitsales = unitsales.join(day)
    unitsales.drop('day', axis=1, inplace=True)

    month = pd.get_dummies(unitsales['month'],prefix='month')
    unitsales = unitsales.join(month)
    unitsales.drop('month', axis=1, inplace=True)
    # Feature Engineering
    family = pd.get_dummies(unitsales['family'])
    unitsales = unitsales.join(family)
    unitsales.drop('family', axis=1, inplace=True)

    state = pd.get_dummies(unitsales['state'])
    unitsales = unitsales.join(state)
    unitsales.drop('state', axis=1, inplace=True)

    unitsales['holidaytype'] = unitsales['holidaytype'].fillna('None')
    unitsales['holidaytype_r'] = unitsales['holidaytype_r'].fillna('None')
    unitsales['holidaytype_n'] = unitsales['holidaytype_n'].fillna('None')

    unitsales['localholiday'] = unitsales.apply(lambda x: holidayflag(x['holidaytype']), axis=1)
    unitsales.drop('holidaytype', axis=1, inplace=True)

    unitsales['regionalholiday'] = unitsales.apply(lambda x: holidayflag(x['holidaytype_r']), axis=1)
    unitsales.drop('holidaytype_r', axis=1, inplace=True)


    unitsales['nationalother'] = unitsales.apply(lambda x: nationalother(x['holidaytype_n']), axis=1)

    unitsales.drop('holidaytype_n', axis=1, inplace=True)


    unitsales['nholidayspike'] = unitsales.apply(lambda x: nholidayspike(x['description_n']), axis=1)

    unitsales['goodfriday'] = unitsales.apply(lambda x: goodfriday(x['description_n']), axis=1)



    unitsales['blackfriday'] = unitsales.apply(lambda x: blackfriday(x['description_n']), axis=1)



    unitsales['worldcupspike'] = unitsales.apply(lambda x: worldcupspike(x['description_n']), axis=1)


unitsales['worldcupdrop'] = unitsales.apply(lambda x: worldcupdrop(x['description_n']), axis=1)

def earthquakespike(x):
    list=['Terremoto Manabi+1','Terremoto Manabi+2','Terremoto Manabi+3','Terremoto Manabi+4','Terremoto Manabi+8','Terremoto Manabi+14','Terremoto Manabi+15']
    if (x.isin(list)):
        return 1
    else:
        return 0

unitsales['earthquakespike'] = unitsales.apply(lambda x: earthquakespike(x['description_n']), axis=1)


    unitsales['earthquakedrop'] = unitsales.apply(lambda x: earthquakedrop(x['description_n']), axis=1)

    unitsales.drop('description_n', axis=1, inplace=True)

    # Drop unnecessary features
    unitsales.drop('date', axis=1, inplace=True)
    unitsales.drop('city', axis=1, inplace=True)
    unitsales.drop('class', axis=1, inplace=True)
    unitsales.drop('item_nbr', axis=1, inplace=True)
    unitsales.drop('store_nbr', axis=1, inplace=True)


    return unitsales




# Process files
for index, row in itemlist.iterrows():
    item = int(row[0])
    df = clean(item)
    df.to_csv('mergeitems/'+ str(item) + '.csv', index=False)


print('Success!')