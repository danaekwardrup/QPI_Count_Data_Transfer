
import pandas as pd
import numpy as np
from sqlalchemy import MetaData, create_engine
import os

metadata = MetaData()
ptgroup_array = None
protcode_array = None
rec_array = None


#enter current run rec table, then previous run rec table that you wish to compare
tables = ["Recommendations_P202104_final", "Recommendations_P202103_final"]

engine = create_engine(
    'mssql+pyodbc://PEUser:peUSER@192.168.100.106:59647/QCDT_HEDIS_YTD_Recs?driver=ODBC+Driver+17+for+SQL+Server')
# now that engine is initialized, let's open a connection to the database
connection = engine.connect()
metadata.create_all(engine)

#enumerate to allow for us to run current run rec table through the script and then prev run rec table
for index, table in enumerate(tables):
    simp_rec_list = []
    org_id_list = []
    prot_list = []
    prot_code_list = []
    table_df = pd.read_sql_query(
       #f"SELECT TOP 5000 SUBSTRING(Patient, 1, 3) as ptgroup, ProtCode, Recommendation FROM {table} order by NEWID()",
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
    column_df = None

    totals_df['Performance Rate %'] = totals_df['Met'] / (totals_df['Denominator'] - totals_df['Exception'])
    totals_df['Performance Rate %'] = totals_df['Performance Rate %'].fillna(0)

    if index == 0:
        current_run_recs = totals_df.copy()
        totals_df.drop(['ptgroup', 'ProtCode','Met', 'Not Met','Denominator', 'Exclusion', 'Exception', 'Performance Rate %'],\
        axis=1,inplace=True)
    elif index == 1:
        previous_run_recs = totals_df

"""
current_run_recs['ProtCode'] = current_run_recs['ProtCode'].astype('string')
previous_run_recs['ProtCode'] = previous_run_recs['ProtCode'].astype('string')
current_run_recs['ptgroup'] = current_run_recs['ptgroup'].astype('string')
previous_run_recs['ptgroup'] = previous_run_recs['ptgroup'].astype('string')


prot_names_dict = {}
for index_a, i in enumerate(current_run_recs['ProtCode']):
    curr_pg = current_run_recs['ptgroup'][index_a]
    current_val = i
    new_prot = True
    for index_b, x in enumerate(previous_run_recs['ProtCode']):
        prev_pg = previous_run_recs['ptgroup'][index_b]
        prev_val = x
        if curr_pg == prev_pg:
            if current_val in prev_val or prev_val in current_val:
                new_prot = False
                prot_names_dict[current_val] = (prev_val, new_prot)

    if new_prot:
        print(f"NEW PROT: {current_val}")
        prot_names_dict[current_val] = ("UNMATCHED", new_prot)

prot_names_list = prot_names_dict.values()
prev_prot_names_list = []

for i in prot_names_list:
    prev_prot_names_list.append(i[0])

previous_run_recs = previous_run_recs[previous_run_recs.ProtCode.isin(prev_prot_names_list)]

# create df for difference between 2 tables
diff_df = current_run_recs[['ptgroup', 'ProtCode']].copy()

for i in current_run_recs['ProtCode']:
    if current_run_recs['ptgroup'].equals(previous_run_recs['ptgroup']):
        diff_df['Met']= current_run_recs['Met'] - previous_run_recs['Met']
        diff_df['Not Met'] = current_run_recs['Not Met'] - previous_run_recs['Not Met']
        diff_df['Denominator'] = current_run_recs['Denominator'] - previous_run_recs['Denominator']
        diff_df['Exclusion'] = current_run_recs['Exclusion'] - previous_run_recs['Exclusion']
        diff_df['Exception'] = current_run_recs['Exception'] - previous_run_recs['Exception']
        diff_df['Performance Rate %'] = current_run_recs['Performance Rate %'] - previous_run_recs['Performance Rate %']
"""

#change perf rate column to % for all three df's

current_run_recs['Performance Rate %'] = current_run_recs['Performance Rate %'].astype(float).map(lambda n: '{:.1%}'.format(n))
previous_run_recs['Performance Rate %'] = previous_run_recs['Performance Rate %'].astype(float).map(lambda n: '{:.1%}'.format(n))
#diff_df['Performance Rate %'] = diff_df['Performance Rate %'].astype(float).map(lambda n: '{:.1%}'.format(n))


#combine the three df's
combined_df = pd.concat([current_run_recs,previous_run_recs], axis=1)

#insert empty columns
combined_df.insert(8, '', '', allow_duplicates=True)
#combined_df.insert(17, '', '', allow_duplicates=True)


desktop = os.path.join(os.path.join(os.environ['USERPROFILE']), 'Desktop')

file_path = os.path.join(desktop, "run_qa.xlsx")

combined_df.to_excel(file_path, sheet_name='Sheet1')


connection.close()
