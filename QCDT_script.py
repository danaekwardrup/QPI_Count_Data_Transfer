#create the engine
import pyodbc
from sqlalchemy import MetaData, create_engine, Table, Column, Unicode
from sqlalchemy.sql import select
metadata = MetaData()




#print out contents of recommendations table for testing purposes
recs = Table('Recommendations_P202009_final', metadata,
            Column('ProtCode', Unicode(50)),
            Column('Patient', Unicode(50)),
            Column('Recommendation', Unicode(255)))

engine = create_engine('mssql+pyodbc://PEUser:peUSER@192.168.100.106:59647/Recommendations_SBGS_YTD?driver=ODBC+Driver+17+for+SQL+Server')
#now that engine is initialized, let's open a connection to the database
connection = engine.connect()
metadata.create_all(engine)

s = select([recs])
rp = connection.execute(s)
#results = rp.fetchall()
#print(results)

for i in rp:
    key = ': current'
    if key in i[2].lower():
        print(i[2])

