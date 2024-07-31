from datetime import datetime
import pandas as pd
import numpy as np
import requests
from bs4 import BeautifulSoup
import sqlite3

log_file = "./small_projects/logs/code_log.txt"

# logging function
def log_progress(message):
    time_format = "%Y-%b-%d %H:%M:%S"
    logging_time = datetime.now()
    logging_str_time = logging_time.strftime(time_format)
    with open(log_file, "a") as file:
        file.write(f"{logging_str_time} : bs4 : {message}\n")



log_progress("Declaring known values")

url = "http://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks"
csv_url = "http://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMSkillsNetwork-PY0221EN-Coursera/labs/v2/exchange_rate.csv"
output_path_csv = "./small_projects/results/Largest_banks_data.csv" 
database_name = "./small_projects/sql_results/Banks.db"
table_name = "Largest_banks"

log_progress("Preliminaries complete. Initiating ETL process")


# extract function
def extract(url):
    # get text from url
    html_text = requests.get(url).text

    # parse html
    data = BeautifulSoup(html_text, "html.parser")

    # find all the tables
    tables = data.find_all("tbody")

    # select first table and find all rows
    rows = tables[0].find_all("tr")
    df_created = False

    # get records from each row
    for row in rows:
        # get column data in the current row
        values = row.find_all("td")

        # if column data is there
        if values:
            dict_values = { "Name" : values[1].text.strip(),
                            "MC_USD_Billion": values[2].text.strip()}
            
            # create a dataframe from data
            df1 = pd.DataFrame(dict_values, index=[0])

            # union two dataframes if already some data is there in the dataframe else assign same dataframe
            df = pd.concat([df, df1], ignore_index=True) if df_created else df1

            df_created = True

    return df

# transformation function
def transform(df, csv_path):

    # read csv file from url
    df_exchange = pd.read_csv(csv_path)

    # convert dataframe to dictionary to acquire exchange rates
    exchange_rate = df_exchange.set_index('Currency').to_dict()['Rate']

    # convert data type to float to make calculations
    df['MC_USD_Billion'] = df['MC_USD_Billion'].astype('float')

    # compute 3 columns for valuations in different currencies
    df['MC_EUR_Billion'] = np.round(df['MC_USD_Billion']*exchange_rate['EUR'],2)
    df['MC_GBP_Billion'] = np.round(df['MC_USD_Billion']*exchange_rate['GBP'],2)
    df['MC_INR_Billion'] = np.round(df['MC_USD_Billion']*exchange_rate['INR'],2)
    
    return df

# save dataframe to csv
def load_to_csv(df, output_path):
    df.to_csv(output_path)

# save dataframe to table
def load_to_db(df, sql_connection, table_name):
    df.to_sql(table_name, sql_connection, if_exists='replace', index=False)


# run queries on sql table
def run_query(sql_query, sql_connection):
    print(sql_query, "\n")
    cursor = sql_connection.cursor()
    cursor.execute(sql_query)
    rows=cursor.fetchall()
    print(rows, "\n")


log_progress("Call extract() function")
df = extract(url)
log_progress("Data extraction complete. Initiating Transformation process")

log_progress("Call transform() function")
df = transform(df, csv_url)
log_progress("Data transformation complete. Initiating Loading process")

log_progress("Call load_to_csv()")
load_to_csv(df,output_path_csv)
log_progress("Data saved to CSV file")

log_progress("Initiate SQLite3 connection")
conn = sqlite3.connect(database_name)
log_progress("SQL Connection initiated")

log_progress("Call load_to_db()")
load_to_db(df, conn, table_name)
log_progress("Data loaded to Database as a table, Executing queries")

log_progress("Close SQLite3 connection")
conn.close()
log_progress("Server Connection closed")

conn = sqlite3.connect(database_name)
run_query("SELECT * FROM Largest_banks", conn)
run_query("SELECT AVG(MC_GBP_Billion) FROM Largest_banks", conn)
run_query("SELECT Name from Largest_banks LIMIT 5", conn)
conn.close()

