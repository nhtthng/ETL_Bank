import pandas as pd
from bs4 import BeautifulSoup
import numpy as np
from datetime import datetime
import sqlite3
import requests

URL = 'https://web.archive.org/web/20230908091635 /https://en.wikipedia.org/wiki/List_of_largest_banks'

table_attributes = ['Name','MC_USD_Billion']

Output_Csv_path = 'D:\\Study\\ETL\\ETL_Bank\\Largest_banks_data.csv'

table_name = 'Largest_banks'

log_file = 'D:\\Study\\ETL\\ETL_Bank\\code_log.txt'



def extract():
    df = pd.DataFrame(columns= table_attributes)
    respones = requests.get(URL).text
    html_page = BeautifulSoup(respones,'html.parser')
    tbody = html_page.find_all('tbody')
    rows = tbody[0].find_all('tr')
    for row in rows:
        col = row.find_all('td')
        if len(col) != 0:
            anchor_tags = col[1].find_all('a')
            if len(anchor_tags) != 0:
                bank_name = anchor_tags[1].contents[0]
                market_cap = col[2].contents[0]
                data_dict = {'Name': bank_name,'MC_USD_Billion': market_cap}
                df1 = pd.DataFrame(data_dict,index=[0])
                df = pd.concat([df,df1],ignore_index=True)
    return df

def transform(df):
    dict = pd.read_csv('ETL_Bank\\exchange_rate.csv')
    data = dict.set_index('Currency').to_dict()['Rate']
    df['MC_USD_Billion'] = df['MC_USD_Billion'].astype(float)
    df['MC_GBP_Billion'] = [np.round(x * data['GBP'],2) for x in df['MC_USD_Billion']]
    df['MC_EUR_Billion'] = [np.round(x * data['EUR'],2) for x in df['MC_USD_Billion']]
    df['MC_INR_Billion'] = [np.round(x * data['INR'],2) for x in df['MC_USD_Billion']]
    return df
    
def load_to_csv(file_path,df):
    df.to_csv(file_path)

def load_to_db(df):
    conn = sqlite3.connect('Banks.db')
    df.to_sql(table_name,conn,if_exists='replace',index = False)
    conn.close()
    
def run_query(query):
    conn = sqlite3.connect('Banks.db')
    print(query)
    value = pd.read_sql(query,conn)
    print(value)
    conn.close()

def log_progress(mess):
    timestamp = '%Y-%h-%d-%H:%M:%S'
    now = datetime.now()
    times = now.strftime(timestamp)
    with open(log_file,'a') as f:
        f.write(times + ',' + mess + '\n')
        
# FIGHT

log_progress('Preliminaries complete. Initiating ETL process')

extract_data = extract()
print('extract_data')
print(extract_data)

log_progress('Data extraction complete. Initiating Transformation process')

tranformed_data = transform(extract_data)
print('tranformed_data')
print(tranformed_data)

log_progress('Data transformation complete. Initiating Loading process')

load_to_csv(Output_Csv_path,tranformed_data)

log_progress('Data saved to CSV file')

log_progress('SQL Connection initiated')

load_to_db(tranformed_data)

log_progress('Data loaded to Database as a table, Executing queries')

run_query(f'SELECT * FROM {table_name}')
run_query(f'SELECT AVG(MC_GBP_Billion) FROM {table_name}')
run_query(f'SELECT Name from {table_name} LIMIT 5')

log_progress('Process Complete')

log_progress('Server Connection closed')