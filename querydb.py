import os as os
from pathlib import Path
from datetime import datetime
import pandas as pd
from enum import Enum
import sqlite3
import jsons

from mytrace import Trace

# the db contains the top 15 deaths for each month, which means the list of causes can
# potentially vary.  In order to create a reasonable chart I select the top 10 causes for
# the entire 20 year period

query = 'SELECT t1.date, t2.rank, t1.cause, t1.deaths '\
'        FROM top15cod AS t1'\
'        JOIN (SELECT ROW_NUMBER() OVER() AS rank, cause'\
'             FROM (SELECT CAUSE FROM top15cod'\
'	                    GROUP BY CAUSE'\
'	                    HAVING COUNT(cause)=240'\
'		                ORDER BY SUM(deaths) DESC'\
'	                    LIMIT 10)) AS t2'\
'        ON t1.cause = t2.cause'\
'        ORDER BY date, rank'

with sqlite3.connect('cdc.sqlite') as cxn:
    dict = pd.read_sql_query( query,cxn).to_dict()

# write serialized representation of required data structure for top10.html
with open('top10.js','w') as f:
    f.write("top10={};\n".format(
        jsons.dumps([ Trace(dict['cause'][i],
                    [dict['date'][l*10] for l in range(240)],
                    [dict['deaths'][l*10+i] for l in range(240)])
                for i in range(10)])))

