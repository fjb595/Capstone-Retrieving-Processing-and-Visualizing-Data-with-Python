import pandas as pd
import os
import sqlite3
import jsons

from mytrace import Trace

filename = 'United_States_COVID-19_Cases_and_Deaths_by_State_over_Time.csv'
query = 'select "2020-" || substr(submission_date,1,2) date, sum(new_death) deaths '\
            'from covid19 '\
            'group by date '\
            'order by date '

with sqlite3.connect('cdc.sqlite') as cxn:
    cursor = cxn.cursor()
    cursor.execute('''DROP TABLE IF EXISTS covid19 ''')


    pd.read_csv(os.path.join(".\\","data",filename)).to_sql("covid19",cxn)
    dict = pd.read_sql_query( query,cxn).to_dict()

    with open('covid19.js','w') as f:
        f.write("covid19={};\n".format(
            jsons.dumps([Trace('SARS-CoV-2',
                               [*dict['date'].values()],
                               [*dict['deaths'].values()])])))
