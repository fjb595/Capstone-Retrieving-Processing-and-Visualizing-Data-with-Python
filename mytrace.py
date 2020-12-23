# data structure required by plotly
class Trace:
    stackgroup = 'one'
    def __init__(self,name,x,y):
        self.name = name
        self.x = x
        self.y = y