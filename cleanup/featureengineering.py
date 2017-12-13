import pandas as pd
import numpy as np
import datetime
    
unitsales = pd.read_csv('../data/train_SampleforEDA.csv', header=0, low_memory = True)

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
    if (x or y or z):
        return 1
    else:
        return 0


# Remove outliers
unitsales = unitsales[unitsales['unit_sales']<1000]
unitsales = unitsales[unitsales['unit_sales']>-1000]

# Create Dummy Variables
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
unitsales.drop('date', axis=1, inplace=True)
unitsales.drop('transferred', axis=1, inplace=True)
unitsales.drop('transferred_n', axis=1, inplace=True)
unitsales.drop('transferred_r', axis=1, inplace=True)
unitsales.drop('locale_n', axis=1, inplace=True)
unitsales.drop('locale_r', axis=1, inplace=True)
unitsales.drop('locale', axis=1, inplace=True)
unitsales.drop('locale_name', axis=1, inplace=True)
    

unitsales.to_csv('train_SampleforClustering.csv', index=False)
