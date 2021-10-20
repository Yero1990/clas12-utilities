import requests
import subprocess
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
  def __init__(self, datetime_str=None):
    self.pvs={}
    self.datetime=None
    self.date=None
    self.time=None
    if datetime_str:
      self.setTime(datetime_str)

  def setTime(self, datetime_str):
    self.datetime = datetime.strptime(datetime_str, "%Y-%m-%dT%H:%M:%S") 
    self.date = self.datetime.strftime("%Y-%m-%d")
    self.time = self.datetime.strftime("%H:%M:%S")

  def addPv(self,name,value):
    self.pvs[name]=value
  def getValue(self,name):
    if name in self.pvs:
      return self.pvs[name]
    else:
      return None
  def __str__(self):
    print(str(self.date),str(self.time),str(self.pvs))

  def __str__(self):
    s = "Date: {}\nTime: {}\n".format(self.date, self.time)
    for pv in self.pvs:
      s += "> pv: {}\tvalue: {}\n".format(pv, self.pvs[pv])
    return s

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

  def get(self, test=True):
    data = []
    if test:
      cmd=['myData','-b',self.start,'-e',self.end,'-i']
      cmd.extend([pv.getMyaDataArg() for pv in self.pvs])
      for line in subprocess.check_output(cmd).splitlines():
        columns=line.strip().split()
        if len(columns) == 2+len(self.pvs):
          date,time=columns[0],columns[1]
          md=MyaDatum(date,time)
          for ii in range(2,len(columns)):
            md.addPv(self.pvs[ii-2].name,columns[ii])
          data.append(md)
      return data
    else:

      data=[]

      # create a empty myadatum which will hold the first value from each pv
      data.append(MyaDatum())
      print(data[0])
      
      for pv in self.pvs:
        params = {
          'b': self.start,
          'e': self.end,
          'c': pv.name,
          'p': 1  # prior point
        }

        # send get request to API for specific pv/channel
        query = requests.get(self.url, params=params)

        # iterate over the data in request, add a myadatum
        print(params['c'])
        first = True
        for item in query.json()['data']:
          timestamp = item['d']
          value = item['v']
          md=MyaDatum(timestamp)
          md.addPv(pv.name, value)  # could add the validation here

          if first:
            print('First!')
            print(pv.name, timestamp, value)
            if not data[0].datetime:
              data[0].setTime(timestamp)

            data[0].addPv(pv.name, value)
          
            first = False
          else:
            data.append(md)

      # first value in this list should have a data value for each pv
      data_sorted = sorted(data, key=attrgetter('datetime'))
      return data_sorted
