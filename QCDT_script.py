#create the engine
import pyodbc
from sqlalchemy import MetaData, create_engine, Table, Column, Unicode
from sqlalchemy.sql import select
metadata = MetaData()




#print out contents of this table Recommendations_P202103_first for testing purposes
rec = Table('Recommendations_P202103_first', metadata,
            Column('Organization', Unicode(50)),
            Column('Patient', Unicode(50)),
            Column('Recommendation', Unicode(255)))

engine = create_engine('mssql+pyodbc://PEUser:peUSER@192.168.100.106:59647/Recommendations_PHFT_YTD?driver=ODBC+Driver+17+for+SQL+Server')
#now that engine is initialized, let's open a connection to the database
connection = engine.connect()
metadata.create_all(engine)

s = select([rec])
rp = connection.execute(s)
results = rp.fetchall()
print(results)


