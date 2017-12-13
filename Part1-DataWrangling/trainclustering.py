import pandas as pd

unitsales = pd.read_csv('train_keep.csv', header=0, low_memory=False)

clusters = pd.read_csv('class_clusers.csv', header=0)
data = unitsales.merge(clusters, how='left', on='class')


print('clusers assigned')

data0 = data[data['label']==0].drop('label', axis=1)
data1 = data[data['label']==1].drop('label', axis=1)
data2 = data[data['label']==2].drop('label', axis=1)
data3 = data[data['label']==3].drop('label', axis=1)
data4 = data[data['label']==4].drop('label', axis=1)
data5 = data[data['label']==5].drop('label', axis=1)
data6 = data[data['label']==6].drop('label', axis=1)
data7 = data[data['label']==7].drop('label', axis=1)
data8 = data[data['label']==8].drop('label', axis=1)
data9 = data[data['label']==9].drop('label', axis=1)

print('writing csvs')

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
