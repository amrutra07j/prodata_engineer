import numpy as np
import pandas as pd



URL = "http://web.archive.org/web/20230902185326/https://en.wikipedia.org/wiki/List_of_countries_by_GDP_%28nominal%29"

# read html
df = pd.read_html(URL)
tables = df[3]


# rename columns with integers
tables.columns = range(tables.shape[1])

# select specific column
eco_data = tables[[0,2]]

# select top 10 rows
eco_data = eco_data.iloc[1:11]
eco_data.columns = ["Country", "GDP_Millions"]


# converting to billions
eco_data["GDP_Millions"] = eco_data["GDP_Millions"].astype("int")
eco_data["GDP_Billions"] = np.round(eco_data["GDP_Millions"]/1000, 2)
eco_data = eco_data.iloc[:, 0:4:2]

# writing_file
eco_data.to_csv("small_projects/results/economic_data.csv")
import os
print(os.getcwd())

print(eco_data)


