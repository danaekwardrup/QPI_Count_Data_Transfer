# create the engine
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
prot_code_list = []

tables = ["Recommendations_P202104_final", "Recommendations_P202103_final"]

engine = create_engine(
    'mssql+pyodbc://PEUser:peUSER@192.168.100.106:59647/Recommendations_HEDIS_YTD?driver=ODBC+Driver+17+for+SQL+Server')
# now that engine is initialized, let's open a connection to the database
connection = engine.connect()
metadata.create_all(engine)

for index, table in enumerate(tables):
    table_df = pd.read_sql_query(
        f"SELECT SUBSTRING(Patient, 1, 3) as ptgroup, ProtCode, Recommendation FROM {table}",
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

    column_df = pd.DataFrame(table_list, columns=['ptgroup', 'ProtCode', 'Recom'])
    column_df['Met'] = np.where(column_df['Recom'] == 'current', 1, 0)
    column_df['Not Met'] = np.where(column_df['Recom'] == 'invalid', 1, 0)
    column_df['Denominator'] = np.where(column_df['Recom'] == 'incl', 1, 0)
    column_df['Exclusion'] = np.where(column_df['Recom'] == 'excl', 1, 0)
    column_df['Exception'] = np.where(column_df['Recom'] == 'exception', 1, 0)

    totals_df = column_df.groupby(['ptgroup', 'ProtCode'], as_index=False).sum()

    totals_df['Performance Rate %'] = totals_df['Met'] / (totals_df['Denominator'] - totals_df['Exception'])
    totals_df['Performance Rate %'] = totals_df['Performance Rate %'].fillna(0)


    if index == 0:
        current_run_recs = totals_df
    elif index == 1:
        previous_run_recs = totals_df

# create df for difference between 2 tables
diff_df = current_run_recs[['ptgroup', 'ProtCode']].copy()

for i in current_run_recs['ProtCode']:
    if current_run_recs['ptgroup'].equals(previous_run_recs['ptgroup']):
        diff_df['Met']= current_run_recs['Met']-previous_run_recs['Met']
        diff_df['Not Met'] = current_run_recs['Not Met'] - previous_run_recs['Not Met']
        diff_df['Denominator'] = current_run_recs['Denominator'] - previous_run_recs['Denominator']
        diff_df['Exclusion'] = current_run_recs['Exclusion'] - previous_run_recs['Exclusion']
        diff_df['Exception'] = current_run_recs['Exception'] - previous_run_recs['Exception']
        diff_df['Performance Rate %'] = current_run_recs['Performance Rate %'] - previous_run_recs['Performance Rate %']

#change perf rate column to % for all three df's
current_run_recs['Performance Rate %'] = current_run_recs['Performance Rate %'].astype(float).map(lambda n: '{:.1%}'.format(n))
previous_run_recs['Performance Rate %'] = previous_run_recs['Performance Rate %'].astype(float).map(lambda n: '{:.1%}'.format(n))
diff_df['Performance Rate %'] = diff_df['Performance Rate %'].astype(float).map(lambda n: '{:.1%}'.format(n))
print(current_run_recs)
print(previous_run_recs)
print(diff_df)



connection.close()
