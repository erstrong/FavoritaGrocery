import pandas as pd
from sklearn.cluster import KMeans
import pickle
from sklearn.externals import joblib

data = pd.read_csv('train_clean.csv')

kmeanModel = KMeans(n_clusters=10)
kmeanModel.fit(data)

with open('unitsaleskmeans.pkl', 'wb') as f:
    joblib.dump(kmeanModel, f)

data['label'] = kmeanModel.labels_

data0 = data[data['label']==0]
data1 = data[data['label']==1]
data2 = data[data['label']==2]
data3 = data[data['label']==3]
data4 = data[data['label']==4]
data5 = data[data['label']==5]
data6 = data[data['label']==6]
data7 = data[data['label']==7]
data8 = data[data['label']==8]
data9 = data[data['label']==9]


data0.to_csv('train_cluster0.csv', index=False)
data1.to_csv('train_cluster1.csv', index=False)
data2.to_csv('train_cluster2.csv', index=False)
data3.to_csv('train_cluster3.csv', index=False)
data4.to_csv('train_cluster4.csv', index=False)
data5.to_csv('train_cluster5.csv', index=False)
data6.to_csv('train_cluster6.csv', index=False)
data7.to_csv('train_cluster7.csv', index=False)
data8.to_csv('train_cluster8.csv', index=False)
data9.to_csv('train_cluster9.csv', index=False)

Print('Success!')
