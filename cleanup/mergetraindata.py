import pandas as pd
import numpy as np



# Get list of item numbers
itemlist = pd.read_csv('itemlist.csv', header=None)

def clean(item):
    print(str(item))
    filename= 'mergeitems/'+ str(item) + '.csv'
    return pd.read_csv(filename, header=0, low_memory=False)




columns = ['date','store_nbr','item_nbr','unit_sales','onpromotion','family','class','perishable','city','state','type','cluster','transactions','dcoilwtico','holidaytype_n','locale_n','locale_name','description_n','transferred_n','holidaytype_r','locale_r','description_r','transferred_r','holidaytype','locale','description','transferred','day','month','year']

output = pd.DataFrame(columns=columns)


for index, row in itemlist.iterrows():
    item = int(row[0])
    output = output.append(clean(item), ignore_index=True)

print('writing full csv')
output.to_csv('train_finalclean.csv', index=False)

print('writing sample csv')
eda=output.sample(n=1000000)
eda.to_csv('train_edasample.csv', index=False)

print('Success!')