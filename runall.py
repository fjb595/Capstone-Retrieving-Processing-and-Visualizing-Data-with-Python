
import webbrowser
import os

# perform all processing

import scandl
import querydb
import covid

# display the visualization
webbrowser.open('file://' +os.path.realpath(os.path.join(".\\","top10.html")).replace("\\","/"))

