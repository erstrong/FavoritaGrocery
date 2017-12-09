import pandas as pd
from datetime import datetime

# Read data file
df = pd.read_csv('transactions.csv', low_memory=False)

# Convert to pivot table
df2 = df.pivot_table(index='date', columns=['store_nbr'], values=['transactions'])
flattened = pd.DataFrame(df2.to_records())
flattened.columns = [hdr.replace("('transactions', ","").replace(")", "") for hdr in flattened.columns]

# Fill in missing dates
flattened['date'] =  pd.to_datetime(flattened['date'], format='%Y/%m/%d')

flattened.set_index('date', inplace=True)

flattened = flattened.resample('D').reset_index()


# Interpolate missing values for stores that existed on 1/2/13
for i in range(1, 20):
    flattened[str(i)] = flattened[str(i)].interpolate(method='linear').bfill().ffill()

for i in range(23, 29):
    flattened[str(i)] = flattened[str(i)].interpolate(method='linear').bfill().ffill()

for i in range(30, 36):
    flattened[str(i)] = flattened[str(i)].interpolate(method='linear').bfill().ffill()

for i in range(37, 42):
    flattened[str(i)] = flattened[str(i)].interpolate(method='linear').bfill().ffill()

for i in range(43, 52):
    flattened[str(i)] = flattened[str(i)].interpolate(method='linear').bfill().ffill()

flattened['54'] = flattened['54'].interpolate(method='linear').bfill().ffill()

# Interpolate missing values for stores opened after 1/2/13
#20 2015-02
t20 = flattened[['date','20']]
t20 = t20[t20['date']>='2015-02-13']
t20['20'] = t20['20'].interpolate(method='linear').bfill().ffill()
flattened['20'] = t20['20']

#21 2015-07
t21 = flattened[['date','21']]
t21 = t21[t21['date']>='2015-07-24']
t21['21'] = t21['21'].interpolate(method='linear').bfill().ffill()
flattened['21'] = t21['21']


#22 2015-10
t22 = flattened[['date','22']]
t22 = t22[t22['date']>='2015-10-09']
t22['22'] = t22['22'].interpolate(method='linear').bfill().ffill()
flattened['22'] = t22['22']


#29 2015-03
t29 = flattened[['date','29']]
t29 = t29[t29['date']>='2015-03-20']
t29['29'] = t29['29'].interpolate(method='linear').bfill().ffill()
flattened['29'] = t29['29']


#36 2013-05
t36 = flattened[['date','36']]
t36 = t36[t36['date']>='2013-05-10']
t36['36'] = t36['36'].interpolate(method='linear').bfill().ffill()
flattened['36'] = t36['36']


#42 2015-08
t42 = flattened[['date','42']]
t42 = t42[t42['date']>='2015-08-21']
t42['42'] = t42['42'].interpolate(method='linear').bfill().ffill()
flattened['42'] = t42['42']


#52 2017-04
t52 = flattened[['date','52']]
t52 = t52[t52['date']>='2017-04-20']
t52['52'] = t52['52'].interpolate(method='linear').bfill().ffill()
flattened['52'] = t52['52']


#53 2014-05
t53 = flattened[['date','53']]
t53 = t53[t53['date']>='2014-05-29']
t53['53'] = t53['53'].interpolate(method='linear').bfill().ffill()
flattened['53'] = t53['53']

flattened.to_csv('transactions_clean.csv', index=False)
print('Success!')
