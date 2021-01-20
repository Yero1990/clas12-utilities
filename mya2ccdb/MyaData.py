import requests
from datetime import datetime
from operator import attrgetter

class MyaPv:
  def __init__(self,name,deadband=None):
    self.name=name
    self.deadband=None
    if deadband is not None:
      self.deadband=float(deadband)
  def getMyaDataArg(self):
    if self.deadband is None:
      return self.name
    else:
      return self.name+','+str(self.deadband)

class MyaDatum:
  def __init__(self,datetime_str):
    self.datetime = datetime.strptime(datetime_str, "%Y-%m-%dT%H:%M:%S") 
    self.date = self.datetime.strftime("%Y-%m-%d")
    self.time = self.datetime.strftime("%H:%M")
    self.pvs={}

  def addPv(self,name,value):
    self.pvs[name]=value
  def getValue(self,name):
    if name in self.pvs:
      return self.pvs[name]
    else:
      return None
  def __str__(self):
    print(str(self.date),str(self.time),str(self.pvs))

class MyaData:
  def __init__(self,start=None,end=None):
    self.pvs=[]
    self.start='-1w'
    self.end='0'
    self.url = 'https://myaweb.acc.jlab.org/myquery/interval'
    if start is not None:
      self.start=str(start)
    if end is not None:
      self.end=str(end)

  def addPv(self,name,deadband=None):
    self.pvs.append(MyaPv(name,deadband))

  def setStart(self,start):
    self.start=str(start)

  def setEnd(self,end):
    self.end=str(end)

  def get(self):
    data=[]

    for pv in self.pvs:
      params = {
        'b': self.start,
        'e': self.end,
        'c': pv.name
      }

      # send get request to API for specific pv/channel
      query = requests.get(self.url, params=params)

      # iterate over the data in request, add a myadatum
      print(params['c'])
      for item in query.json()['data']:
        timestamp = item['d']
        value = item['v']
        md=MyaDatum(timestamp)
        md.addPv(pv, value)  # could add the validation here
        data.append(md)
    
    data_sorted = sorted(data, key=attrgetter('datetime'))
    return data_sorted
