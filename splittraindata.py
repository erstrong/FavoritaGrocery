import pandas as pd
import numpy as np

# Read Data Files
unitsales = pd.read_csv('train.csv', low_memory=False)
#transactions = pd.read_csv('transactions_clean.csv')

unitsales.drop('onpromotion', axis=1, inplace=True)
unitsales.drop('id', axis=1, inplace=True)

# Get list of item numbers
itemlist = unitsales.item_nbr.unique()
np.savetxt("itemlist.csv", itemlist, delimiter=",")



def split(item):
    df = unitsales[unitsales['item_nbr']==item]
    file = 'items/' + str(item) + '.csv'
    df.to_csv(file, index=False)

for item in itemlist:
    split(item)


print('Success!')