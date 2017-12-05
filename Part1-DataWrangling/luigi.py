import pandas as pd
import numpy as np
from datetime import datetime
import datetime
import luigi
import os

class cleanOilData(luigi.Task):
	def run(self):
		oils=pd.read_csv('oil.csv')
		oils['date'] =  pd.to_datetime(oils['date'], format='%Y/%m/%d')
		oils.set_index('date', inplace=True)
		oils = oils.resample('D').mean().reset_index()
		oils=oils.interpolate(method='linear').bfill().ffill()
		oils.to_csv('oils_clean.csv', index=False)
	def output(self):
	
class cleanTransactionsData(luigi.Task):
	def run(self):
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
	def output(self):
	
	
class splitTrainData(luigi.Task):
	def run(self):
	unitsales = pd.read_csv('train.csv', low_memory=False)
#transactions = pd.read_csv('transactions_clean.csv')

#unitsales.drop('onpromotion', axis=1, inplace=True)
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
	def output(self):

class cleanTrainData(luigi.Task):
	def requires(self):
	def run(self):
		transactions = pd.read_csv('transactions_clean.csv')
		stores = pd.read_csv('stores.csv', header=0)
		transactions = pd.read_csv('transactions_clean.csv', header=0)
		items = pd.read_csv('items.csv', header=0)
		holidays = pd.read_csv('holidays_events.csv', header=0)
		oil = pd.read_csv('oils_clean.csv', header=0)

# Get list of item numbers
		itemlist = pd.read_csv('itemlist.csv', header=None)


# Preprocessing
		holidays = holidays.rename(columns={'date':'date','type':'holidaytype','locale':'locale','locale_name':'locale_name',
                           'description':'description','transferred':'transferred'})
		nationalholidays = holidays[holidays['locale']=='National']
		regionalholidays = holidays[holidays['locale']=='Regional']
		localholidays = holidays[holidays['locale']=='Local']

		localholidays = localholidays.rename(columns={'date':'date','holidaytype':'holidaytype','locale':'locale','locale_name':'city','description':'description','transferred':'transferred'})
		regionalholidays = regionalholidays.rename(columns={'date':'date','holidaytype':'holidaytype','locale':'locale','locale_name':'state','description':'description','transferred':'transferred'})


		transactions = transactions.melt(id_vars=['date'])
		transactions = transactions.rename(columns={'date':'date','variable':'store_nbr','value':'transactions'})
		transactions['store_nbr'] = transactions['store_nbr'].map(lambda x: int(x))



		def clean(item):
			print(str(item))
			filename= 'items/'+ str(item) + '.csv'
			df = pd.read_csv(filename)
			df['onpromotion'].fillna(False, inplace=True)
    # Combine with other files
			df = df.merge(items, on='item_nbr', how='left')
			df = df.merge(stores, on='store_nbr', how='left')
    #df = df['store_nbr'].map(lambda a: int(a))
			df = df.merge(transactions, how='left', on=['date','store_nbr'])
			df = df.merge(oil, how='left', on='date')
			df = df.merge(nationalholidays, on='date', how='left')
			df = df.merge(regionalholidays, on=['date','state'], how='left', suffixes=('_n','_r'))
			df = df.merge(localholidays, on=['date','city'], how='left')
    # Convert date to datetime
			format = '%Y-%m-%d'
			df['date'] = df['date'].map(lambda a: datetime.datetime.strptime(a, format))
			df['day'] = df['date'].map(lambda x: x.weekday())
			df['month'] = df['date'].map(lambda x: x.month)
			df['year'] = df['date'].map(lambda x: x.year)
			df.to_csv('mergeitems/'+ str(item) + '.csv', index=False)



		for index, row in itemlist.iterrows():
			item = int(row[0])
			clean(item)
	def output(self):
	
class mergeTrainData(luigi.Task):
	def requires(self):
		yield cleanTrainData()
	def run(self):
		# Get list of item numbers
		itemlist = pd.read_csv('itemlist.csv', header=None)

# Read item csvs into a list of dataframes
		def clean(item):
			print(str(item))
			filename= 'mergeitems/'+ str(item) + '.csv'
			return pd.read_csv(filename, header=0, low_memory=False)

		df_list = []

		for  index, row in itemlist.iterrows():
			item = int(row[0])
			df_list.append(clean(item))

# Concatenate the dataframes
		print('creating dataframe')
		output = pd.concat(df_list)

# Write output csvs
		print('writing full csv')
		output.to_csv(self.output().path,index=False)
	
	def output(self):
		return luigi.LocalTarget('train_SampleForEDA.csv')
	
if __name__=='__main__':
	luigi.run()