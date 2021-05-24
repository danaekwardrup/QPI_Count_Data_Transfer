#create the engine
import pyodbc
import pandas as pd
import numpy as np
from sqlalchemy import MetaData, create_engine, Table, Column, Unicode
from sqlalchemy.sql import select
metadata = MetaData()
ptgroup_array = None
protcode_array = None
rec_array = None
org_id_list = []
prot_list = []
simp_rec_list = []


engine = create_engine('mssql+pyodbc://PEUser:peUSER@192.168.100.106:59647/Recommendations_SBGS_YTD?driver=ODBC+Driver+17+for+SQL+Server')
#now that engine is initialized, let's open a connection to the database
connection = engine.connect()
metadata.create_all(engine)

table_df = pd.read_sql_query(
"SELECT SUBSTRING(Patient, 1, 3) as ptgroup, ProtCode, Recommendation FROM Recommendations_P202009_final",
con=engine
)




for org_id in table_df[['ptgroup']]:
    org_id_obj = table_df[org_id]
    ptgroup_array = org_id_obj.values
    for id in ptgroup_array:
        org_id_list.append(id)


for prot in table_df[['ProtCode']]:
    prot_obj = table_df[prot]
    protcode_array = prot_obj.values
    for prot_name in protcode_array:
        prot_list.append(prot_name)


for rec in table_df[['Recommendation']]:
    Rec_obj = table_df[rec]
    rec_array = Rec_obj.values

    for rec in rec_array:
        if ': current' in rec.lower():
            rec = 'current'
            simp_rec_list.append(rec)

        elif 'excl' in rec.lower():
            rec = 'excl'
            simp_rec_list.append(rec)

        elif 'incl' in rec.lower():
            rec = 'incl'
            simp_rec_list.append(rec)

        elif 'invalid' in rec.lower():
          rec = 'invalid'
          simp_rec_list.append(rec)

        elif 'exception' in rec.lower():
          rec = 'exception'
          simp_rec_list.append(rec)

        else:
          rec = 'other'
          simp_rec_list.append(rec)

table_list = list(zip(org_id_list, prot_list, simp_rec_list))
three_column_df = pd.DataFrame(table_list, columns =['ptgroup', 'ProtCode', 'Recom'])

three_column_df['Met'] = np.where(three_column_df['Recom'] == 'current', 1, 0)

three_column_df['Not Met'] = np.where(three_column_df['Recom'] == 'invalid', 1, 0)

three_column_df['Denominator'] = np.where(three_column_df['Recom'] == 'incl', 1, 0)

three_column_df['Exclusion'] = np.where(three_column_df['Recom'] == 'excl', 1, 0)

three_column_df['Exception'] = np.where(three_column_df['Recom'] == 'exception', 1, 0)

print(three_column_df)





#plz = three_column_df.groupby(['ptgroup', 'ProtCode'])['Recom'].count()

connection.close()
