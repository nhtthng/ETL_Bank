import pandas as pd
dict = pd.read_csv('ETL_Bank\\exchange_rate.csv')
data = dict.set_index('Currency').to_dict()['Rate']
print(data)