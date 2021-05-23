#create the engine
import pyodbc
import pandas as pd
from sqlalchemy import MetaData, create_engine, Table, Column, Unicode
from sqlalchemy.sql import select
counter_current = 0
counter_invalid = 0
counter_incl = 0
counter_excl = 0
counter_excep = 0
metadata = MetaData()


engine = create_engine('mssql+pyodbc://PEUser:peUSER@192.168.100.106:59647/Recommendations_SBGS_YTD?driver=ODBC+Driver+17+for+SQL+Server')
#now that engine is initialized, let's open a connection to the database
connection = engine.connect()
metadata.create_all(engine)

table_df = pd.read_sql_query(
"SELECT SUBSTRING(Patient, 1, 3) as ptgroup, ProtCode, Recommendation FROM Recommendations_P202009_final",
con=engine
)

for rec in table_df[['Recommendation']]:
    Rec_obj = table_df[rec]
    rec_array = Rec_obj.values

for rec in rec_array:
    if ': current' in rec.lower():
        counter_current += 1
    if 'excl' in rec.lower():
        counter_excl += 1
    if 'incl' in rec.lower():
        counter_incl += 1
    if 'invalid' in rec.lower():
        counter_invalid += 1
    if 'exception' in rec.lower():
        counter_excep += 1

print(counter_current)
print(counter_excl)
print(counter_incl)
print(counter_invalid)
print(counter_excep)



connection.close()

#grouped_multiple = table_df.groupby(['ptgroup', 'ProtCode'])
#print(grouped_multiple)