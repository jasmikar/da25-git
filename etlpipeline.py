
# ETL-PIPELINE

import pandas as pd
from snowflake.connector.pandas_tools import write_pandas

from dotenv import load_dotenv
# import sys: Imports the built-in Python module sys, which provides access to system-specific parameters and functions.
# sys.path is a list that contains the directory names where Python looks for modules when importing. 
# insert(0, '..'): Adds a new directory to the beginning of the Python path list (sys.path). Here, '..' refers to the parent directory.
import sys; sys.path.insert(0, '..')
# After modifying the Python path using sys.path.insert(0, '..'), the code then imports the get_conn function from a module named data.
from scripts.data import get_conn


# EXTRACT
df = pd.read_csv("sources/words.csv")
print("EXTRACTED")


# TRANSFORM
df['track_name'] = df['track_name'].astype(str)
df['track_album_release_date'] = pd.to_datetime(df['track_album_release_date'], format='mixed')
print("TRANSFORMED")


# LOAD
# Snowflake login information
load_dotenv("snowflake.env")
# Function with connector to Snowflake. Function is within scripts/data.py 
conn = get_conn()
# A cursor is an abstraction provided by database libraries that allows to interact with a database
cur = conn.cursor()
# In this case executing command delete all rows from table word
cur.execute("DELETE FROM WORD")

write_pandas(conn, df, "WORD", auto_create_table=True, index=False)
print("LOADED")


