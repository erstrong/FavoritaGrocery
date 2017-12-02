import pandas as pd
import numpy as np

# Read Data Files
transactions = pd.read_csv('transactions_clean.csv')


# Get list of item numbers
itemlist = pd.read_csv('itemlist.csv', header=None)
output = pd.DataFrame(transactions['date'])
output['date'] = pd.to_datetime(output['date'])
transactions.set_index('date', inplace=True)
#print(output)

def clean(item):
    print(str(item))
    filename= 'items/'+ str(item) + '.csv'
    df = pd.read_csv(filename)
    df = df.pivot_table(index='date', columns=['store_nbr'], values=['unit_sales'])
    flattened = pd.DataFrame(df.to_records())
    flattened.columns = [hdr.replace("('unit_sales', ","").replace(")", "") for hdr in flattened.columns]
    flattened['date'] =  pd.to_datetime(flattened['date'], format='%Y/%m/%d')
    flattened.set_index('date', inplace=True)
    flattened = flattened.resample('D').mean().reset_index()
    flattened.set_index('date', inplace=True)
    stores = flattened.columns.tolist()
    temp_transactions = transactions[stores]
    for store in stores:
        if (store==20):
            t20 = flattened['20']
            t20 = t20.loc['2015-02-13':]
            t20['20'] = t20['20'].interpolate(method='linear').bfill().ffill()
            flattened['20'] = t20['20']
        elif (store==21):
            t21 = flattened['21']
            t21 = t21.loc['2015-07-24':]
            t21['21'] = t21['21'].interpolate(method='linear').bfill().ffill()
            flattened['21'] = t21['21']
        elif (store==22):
            t22 = flattened['22']
            t22 = t22.loc['2015-10-09':]
            t22['22'] = t22['22'].interpolate(method='linear').bfill().ffill()
            flattened['22'] = t22['22']
        elif (store==29):
            t29 = flattened['29']
            t29 = t29.loc['2015-03-20':]
            t29['29'] = t29['29'].interpolate(method='linear').bfill().ffill()
            flattened['29'] = t29['29']
        elif (store==36):
            t36 = flattened['36']
            t36 = t36.loc['2013-05-10':]
            t36['36'] = t36['36'].interpolate(method='linear').bfill().ffill()
            flattened['36'] = t36['36']
        elif (store==42):
            t42 = flattened['42']
            t42 = t42.loc['2015-08-21':]
            t42['42'] = t42['42'].interpolate(method='linear').bfill().ffill()
            flattened['42'] = t42['42']
        elif (store==52):
            t52 = flattened['52']
            t52 = t52.loc['2017-04-20':]
            t52['52'] = t52['52'].interpolate(method='linear').bfill().ffill()
            flattened['52'] = t52['52']
        elif (store==53):
            t53 = flattened['53']
            t53 = t53.loc['2014-05-29':]
            t53['53'] = t53['53'].interpolate(method='linear').bfill().ffill()
            flattened['53'] = t53['53']
        else:
            flattened[store] = flattened[store].interpolate(method='linear').bfill().ffill()
        flattened[store] = flattened[store]/transactions[store]
    flattened[item] = flattened.mean(axis=1)
    flattened['date'] = pd.to_datetime(flattened.index.values)
    columns = ['date', item]
    return flattened[columns]


df_collection=[]

for index, row in itemlist.iterrows():
    item = int(row[0])
    df_collection.append(clean(item))

for df in df_collection:
    df['date'] = pd.to_datetime(df['date'])
    output = output.merge(df, on='date', how='left')

#for index, row in itemlist.iterrows():
    #item = int(row[0])
    #output[item] = output[item].interpolate(method='linear').bfill().ffill()

output.fillna(0, inplace=True)
#output.to_csv('train_finalclean.csv')
output.to_csv('train_zeros.csv')
print('Success!')