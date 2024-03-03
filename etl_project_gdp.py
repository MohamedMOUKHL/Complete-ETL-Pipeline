# Code for ETL operations on Country-GDP data

# Importing the required libraries
from bs4 import BeautifulSoup
import requests
import pandas as pd
import numpy as np
import sqlite3
from datetime import datetime 


url = 'https://web.archive.org/web/20230902185326/https://en.wikipedia.org/wiki/List_of_countries_by_GDP_%28nominal%29'
table_attribs = ["Country", "GDP_USD_millions"]
db_name = 'World_Economies.db'
table_name = 'Countries_by_GDP'
csv_path = './Countries_by_GDP.csv'


def extract(url, table_attribs):
    ''' This function extracts the required
    information from the website and saves it to a dataframe. The
    function returns the dataframe for further processing. '''
    # Extracting the HTML content from the website
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')
    df = pd.DataFrame(columns=table_attribs)
    # Extracting the required information from the HTML content
    table = soup.find_all('tbody')[2]
    rows = table.find_all('tr')
    for row in rows:
        columns = row.find_all('td')
        if columns and len(columns) >= 3:
            country_link = columns[0].find('a')
            if country_link and columns[2].text.strip() != '—':
                data_dict = {"Country": columns[0].a.contents[0],
                             "GDP_USD_millions": columns[2].contents[0]}
                df1 = pd.DataFrame(data_dict, index=[0])
                df = pd.concat([df,df1], ignore_index=True)
    return df

def transform(df):
    ''' This function converts the GDP information from Currency
    format to float value, transforms the information of GDP from
    USD (Millions) to USD (Billions) rounding to 2 decimal places.
    The function returns the transformed dataframe.'''
    GDP_list = df["GDP_USD_millions"].tolist()
    GDP_list = [float("".join(x.split(','))) for x in GDP_list]
    GDP_list = [np.round(x/1000,2) for x in GDP_list]
    df["GDP_USD_millions"] = GDP_list
    df=df.rename(columns = {"GDP_USD_millions":"GDP_USD_billions"})
    return df

def load_to_csv(df, csv_path):
    ''' This function saves the final dataframe as a `CSV` file 
    in the provided path. Function returns nothing.'''
    df.to_csv(csv_path)

def load_to_db(df, sql_connection, table_name):
    ''' This function saves the final dataframe as a database table
    with the provided name. Function returns nothing.'''
    df.to_sql(table_name, sql_connection, if_exists='replace', index=False)

def run_query(query_statement, sql_connection):
    ''' This function runs the stated query on the database table and
    prints the output on the terminal. Function returns nothing. '''
    print(query_statement)
    query_output = pd.read_sql(query_statement, sql_connection)
    print(query_output)

def log_progress(message):
    timestamp_format = '%Y-%h-%d-%H:%M:%S' # Year-Monthname-Day-Hour-Minute-Second 
    now = datetime.now() # get current timestamp 
    timestamp = now.strftime(timestamp_format) 
    with open("./etl_project_log.txt","a") as f: 
        f.write(timestamp + ' : ' + message + '\n')



#Declaring known values
log_progress("ETL Job Started") 

#Call extract() function	
log_progress("Extract phase Started")
extracted_data = extract(url, table_attribs) 
log_progress("Data extraction complete. Initiating Transformation process.") 

#Call transform() function
log_progress("Transform phase Started")
tansformed_data = transform(extracted_data)
log_progress("Data transformation complete. Initiating loading process.")

#Call load_to_csv()
log_progress("Load phase Started")
load_to_csv(tansformed_data, csv_path)
log_progress("Data saved to CSV file.")

#Initiate SQLite3 connection
sql_connection = sqlite3.connect(db_name)
log_progress('SQL Connection initiated.')

#Call load_to_db()
load_to_db(tansformed_data, sql_connection, table_name)
log_progress("Data loaded to Database as table. Running the query.")

#Call run_query()
query_statement = f"SELECT * from {table_name} WHERE GDP_USD_billions >= 100"
run_query(query_statement, sql_connection)
log_progress("Process Complete")

#Close SQLite3 connection
sql_connection.close()