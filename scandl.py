import os as os
from pathlib import Path
from datetime import datetime
import pandas as pd
from enum import Enum
import sqlite3
import zipfile as zp


#
# parses top 15 causes of data  manually exported from https://wonder.cdc.gov/ucd-icd10.html
# and inserts into sqllite
#
# the exported data is expected to be contained in ./data/cdc.zip, consist of set of .txt files
# that were exported using the following query parameters:
# 1. Organize table layout, Group Results By: 15 leading causes of death
# 4. Select year and month: all dates from 1999/01-2019/12, one month per query
# 7. Other options: check 'export results'
#
# Note: The filenames of the resulting .txt files are not significant.
#

key = '"Year/Month: '

class ParseState(Enum):
    SKIP_HEADER = 1
    PARSE_DATA = 2
    SCAN_FOR_DATE = 3

cxn = sqlite3.connect('cdc.sqlite')
cxn.cursor().execute('''DROP TABLE IF EXISTS top15cod ''')

#
# for each txt file in the zip, create a dictionary from the parsed data.
# then convert to a pandas dataframe, which is used to insert to sqllite
#

with zp.ZipFile('./data/cdc.zip') as z:
    for fn in zp.Path(z).iterdir():
        if fn.name.split('.')[-1] == 'txt':
            with z.open(fn.name) as f:
                state = ParseState.SKIP_HEADER

                # accumulate parsed data in a pandas dataframe for subsequent sqllite storage
                causes = []; deaths = []

                for s in f.readlines():
                    # readLines returns bytes for a zip
                    s = s.decode()
                    # handle mainline case first
                    if state == ParseState.PARSE_DATA:
                        if s != '"---"\r\n':
                            #ex:
                            # "#Cerebrovascular diseases (I60-I69)"	"GR113-070"	14710	Not Applicable	Not Applicable

                            parts = s.split("\t")
                            causes.append(parts[1].replace('#', '').replace('"', ''))
                            deaths.append(int(parts[3]))
                        else: #eod marker
                            state = ParseState.SCAN_FOR_DATE
                            continue

                    elif state == ParseState.SKIP_HEADER:
                        # don't need the column names
                        state = ParseState.PARSE_DATA
                        continue
                    elif state == ParseState.SCAN_FOR_DATE: # finished reading data, now scan for the reporting date
                        # ex:
                        # "Year/Month: Feb., 2000"
                        if s.startswith(key): # look for reporting date key
                            date = datetime.strptime(s[len(key):]
                                           .strip()
                                           .replace(".", ""), '%b, %Y"')\
                                           .strftime("%Y-%m")
                            break;

            # insert df into sqllite db.  table will first be created and then subsequent calls
            # will insert
            pd.DataFrame({'date' : [date] * len(causes), 'cause' : causes, 'deaths' : deaths })\
                .to_sql( "top15cod", cxn, if_exists='append', index = True)

cxn.close()