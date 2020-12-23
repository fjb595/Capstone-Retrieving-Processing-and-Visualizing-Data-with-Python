import os as os
from pathlib import Path
from datetime import datetime
import pandas as pd
from enum import Enum
import sqlite3

key = '"Year/Month: '

cxn = sqlite3.connect('cdc.sqlite')
cursor = cxn.cursor()
cursor.execute('''DROP TABLE IF EXISTS top15cod ''')
#cursor.execute('CREATE TABLE IF NOT EXISTS "top15cod" '
#               '("date"	INTEGER NOT NULL,"deaths"	'
#               'INTEGER NOT NULL,"causes"	'
#               'TEXT NOT NULL,PRIMARY KEY("date"))')

for fn in sorted(Path("./data").iterdir(), key=os.path.getmtime):
    if ( fn.suffix.lower() == '.txt'):
        with open(fn, "r") as f:
            # initialize some flags that control the parse

            class ParseState(Enum):
                SKIP_HEADER = 1
                PARSE_DATA = 2
                SCAN_FOR_DATE = 3

            state = ParseState.SKIP_HEADER

            # accumulate parsed data in a pandas dataframe for subsequent sqllite storage
            causes = []; deaths = []

            for s in f.readlines():
                # handle mainline case first
                if state == ParseState.PARSE_DATA:
                    if s != '"---"\n':
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