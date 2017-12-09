import pandas as pd
from sklearn.cluster import KMeans
import pickle
from sklearn.externals import joblib

data = pd.read_csv('output.csv')

data.columns=['id','','','','','','','','']

data.drop('id', axis=1, inplace=True)
data.drop('store_nbr', axis=1, inplace=True)
data.drop('item_nbr', axis=1, inplace=True)
data.drop('holidaytype', axis=1, inplace=True)
data.drop('holidaytype_n', axis=1, inplace=True)
data.drop('holidaytype_r', axis=1, inplace=True)

kmeanModel = KMeans(n_clusters=5)
kmeanModel.fit(data)

data['label'] = kmeanModel.labels_

data0 = data[data['label']==0]
data1 = data[data['label']==1]
data2 = data[data['label']==2]
data3 = data[data['label']==3]
data4 = data[data['label']==4]

data0.to_csv('train_cluster0.csv', index=False)
data1.to_csv('train_cluster1.csv', index=False)
data2.to_csv('train_cluster2.csv', index=False)
data3.to_csv('train_cluster3.csv', index=False)
data4.to_csv('train_cluster4.csv', index=False)
