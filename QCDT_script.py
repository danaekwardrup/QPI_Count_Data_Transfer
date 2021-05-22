#create the engine
import pyodbc
import pandas as pd
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

counter_current = 0
counter_invalid = 0
counter_incl = 0
counter_excl = 0
counter_excep = 0

for i in rp:
    if ': current' in i[2].lower():
        counter_current += 1
    if 'excl' in i[2].lower():
        counter_excl += 1
    if 'incl' in i[2].lower():
        counter_incl += 1
    if 'invalid' in i[2].lower():
        counter_invalid += 1
    if 'exception' in i[2].lower():
        counter_excep += 1

print(counter_current)
print(counter_excl)
print(counter_incl)
print(counter_invalid)
print(counter_excep)

connection.close()