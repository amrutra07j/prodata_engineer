# Fetch top 50 movies and store in sql database and csv file
import requests
import sqlite3
import pandas as pd
from bs4 import BeautifulSoup

#url to fetch data
url = 'https://web.archive.org/web/20230902185655/https://en.everybodywiki.com/100_Most_Highly-Ranked_Films'

# database name with location
db_name = 'small_projects/sql_results/Movies.db'
table_name = 'Top_50'

# path to save csv file
csv_path = 'small_projects/results/top_50_films.csv'

# an empty dataframe to append data
df = pd.DataFrame(columns=["Average Rank","Film","Year"])
count = 0

# get result from web
html_page = requests.get(url).text
data = BeautifulSoup(html_page, 'html.parser')


# find all the elelments and data inside tbody tag
tables = data.find_all('tbody')

# fetch row data from first tbody tag
rows = tables[0].find_all('tr')


# collect data from each row
for row in rows:
    if count<50:
        col = row.find_all('td')
        if len(col)!=0:
            data_dict = {"Average Rank": col[0].contents[0],
                         "Film": col[1].contents[0],
                         "Year": col[2].contents[0]}
            df1 = pd.DataFrame(data_dict, index=[0])
            df = pd.concat([df,df1], ignore_index=True)
            count+=1
    else:
        break

# save to csv file
df.to_csv(csv_path)


# connect to sql database 
conn = sqlite3.connect(db_name)

# write to sql database
df.to_sql(table_name, conn, if_exists='replace', index=False)
conn.close()

