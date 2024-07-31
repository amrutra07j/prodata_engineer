from datetime import datetime
import pandas as pd
import numpy as np
import sqlite3

# Some times this file throws error as html page returned is different. Update extract function df = df[0] in second line


log_file = "./small_projects/logs/code_log.txt"

# logging function
def log_progress(message):
    time_format = "%Y-%b-%d %H:%M:%S"
    logging_time = datetime.now()
    logging_str_time = logging_time.strftime(time_format)
    with open(log_file, "a") as file:
        file.write(f"{logging_str_time} : pandas: {message}\n")


log_progress("Declaring known values")

url = "http://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks"
csv_url = "http://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMSkillsNetwork-PY0221EN-Coursera/labs/v2/exchange_rate.csv"
output_path_csv = "./small_projects/results/Largest_banks_data.csv" 
database_name = "./small_projects/sql_results/Banks.db"
table_name = "Largest_banks"

log_progress("Preliminaries complete. Initiating ETL process")
    
# extract function using pandas 
def extract(url):
    df = pd.read_html(url)
    df = df[1]
    return df

# transformation function
def transform(df, csv_url):

    # read csv from website
    exchange_df = pd.read_csv(csv_url)

    # create 3 columns in df with different currencies
    for i in range(3):
        df[exchange_df.iloc[i,0]] = exchange_df.iloc[i,1]

    # calculate valuation in 3 currencies
    list_currency = ["EUR", "GBP", "INR"]
    for currency in list_currency:
        df[currency] = np.round(df["Market cap (US$ billion)"] * df[currency], 2)

    # remove rank column from dataframe
    df = df.iloc[:, 1:]

    # rename columns
    df.columns = ["name", "MC_USD_Billion", "MC_EUR_Billion", "MC_GBP_Billion", "MC_INR_Billion"]
    return df

# save dataframe to csv
def load_to_csv(df, output_path):
    df.to_csv(output_path)

# save dataframe to tables
def load_to_db(df, sql_connection, table_name):
    df.to_sql(table_name, sql_connection, if_exists='replace', index=False)

# print results for given query
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