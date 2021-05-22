#create the engine
import pyodbc
import pandas as pd
from sqlalchemy import MetaData, create_engine, Table, Column, Unicode
from sqlalchemy.sql import select

metadata = MetaData()


engine = create_engine('mssql+pyodbc://PEUser:peUSER@192.168.100.106:59647/Recommendations_SBGS_YTD?driver=ODBC+Driver+17+for+SQL+Server')
#now that engine is initialized, let's open a connection to the database
connection = engine.connect()
metadata.create_all(engine)

table_df = pd.read_sql(

"SELECT * FROM Recommendations_P202009_final",
con=engine
)
print(table_df.info())