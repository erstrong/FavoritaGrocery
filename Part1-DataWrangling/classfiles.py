import pandas as pd

unitsales = pd.read_csv('train.csv', header=0, low_memory=False)

items = pd.read_csv('items.csv', header=0)
unitsales = unitsales.merge(items, on='item_nbr', how='left')

unitsales = unitsales[unitsales['family']=='GROCERY I'].drop('family', axis=1)

unitsales = unitsales.sample(frac=0.2)
unitsales.to_csv('train_keep.csv', index=False)