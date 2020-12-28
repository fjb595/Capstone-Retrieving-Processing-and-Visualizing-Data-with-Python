
import webbrowser
import os
from pathlib import Path


#
# produces a chart comparing US covid-19 fatalities with other top causes of death
#
# this script is provided as convenience.  the imported scripts can also be executed
# manually
#
# see the imported scripts for details of their operation
#


# scripts assume current dir is script dir
os.chdir(Path(__file__).parent)

# perform all processing

import scandl
import querydb
import covid



# if the chart isn't displayed, open top10.html in a web browser
webbrowser.open('file://' +os.path.realpath(os.path.join(".\\","top10.html")).replace("\\","/"))

