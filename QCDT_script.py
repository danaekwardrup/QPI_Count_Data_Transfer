#create the engine
import pyodbc
from sqlalchemy import create_engine
engine = create_engine('mssql+pyodbc://PEUser:peUSER@192.168.100.26:1433/Recommendations_PHFT_YTD?driver=ODBC+Driver+17+for+SQL+Server')

#now that engine is initialized, let's open a connection to the database
connection = engine.connect()

print(connection)