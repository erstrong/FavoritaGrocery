import pandas as pd

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
output.to_csv('train_finalclean.csv', index=False)

print('writing sample csv')
eda=output.sample(n=1000000)
eda.to_csv('train_SampleForEDA.csv', index=False)

print('Success!')