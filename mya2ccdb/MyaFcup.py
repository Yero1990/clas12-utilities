
from MyaData import MyaDatum
from CcdbUtil import RunRange

_BEAM_ENERGY_TOLERANCE=10  # MeV
_BEAM_STOP_THRESHOLD=10
_RUN_NUMBER_MAX=2E5

#
# Beam blocker attenuation factor is dependent on beam energy.
# Here we hardcode the results of the attenuation measurements,
# since sometimes data is taken without a previous measurement
# available and so the archived PV for attenuation is sometimes
# inaccurate.


_ATTEN={}

# TO run the mya2ccdb.py code, I will need to run each target separately, since the code only supports attenuation of a given beam energy and target
# so will have to uncomment ONLY one: _ATTEN[beam_energy]=att_factor below, for a particular target, and run the mya2ccdb.py code

# C. Yero May 19, 2022 | Work Summary:
# 0) For each of the run ranges below (separated by target changes), I determined the start_time of 1st run and end_time of last run in the range
# 1) ran the mya2ccdb.py code for each range, and made comments regarding any errors mya2ccdb.py might have displayed,
# 2) for each range, provided mya2ccdb.py ran without errors: I made sure to check the MyaPlot GUI  beam_stop and beam_stop_atten, 
#    to make sure they were very close to the attenuation from the table below)
#
# Observations / Questions for Nathan: 
# 0) I noticed some of the run times in the clas12mon.jlab.org/runs/summaries were inconsistent with the run time of PV B_DAQ:run_number
#    (for example, see run 15042, where in clas12mon is says: 11:02:53 - 11:19:40. whereas in B_DAQ_run_number it run time keeps going until ~17:32:18 .   
#    Will use the run range times in clas12mon for now.
#
# 1) from 15178-15317, I found that from MyaPlot, on 11-29-2021, 'beam_stop_atten' changes from 14.8788 to 12.017, whereas the attenuation from table below 
#    is constant at 12.017. I found similar changes in beam_stop_atten on different run ranges as well.  Why ??  Maybe these small changes are OK ?    
#    Similarly, for run range 15390-15432, on Mya Plot, beam_stop_atten changes from 9.5178 to 12.6204, where 9.5178 was the attenuation from the previous
#    run range. So it is as if there is some sort of mismatch between the run ranges and the attenuation factors ???

# 2) I noticed from the output of mya2ccdb, the SLM slope was inconsistent with the SLM slope in MyaPlot (see, for example, Run range: 15178-15317)
#    the faraday cup slope / offsets in the output were consistent with those in the MyaPlot 
#
# 3) Runs 15726, 15727 in clas12mon.jlab.org/runs/summaries, shows it still has beam energy of 2 GeV, so I excluded those. Also, from attenuation 
#    factors table, the range 15726-15732 has no attenuation factor, so I put in 16.40835, presumably since the other Empty runs at this beam energy 
#    have that attenuation
#
# 4) At 2 GeV beam energy, Run range: 15533-15565, MyaPlot beam_stop_atten=13.9358, whereas in the table below, the attenuation factor is 1.000, presumably
#    beacuse the beam energy is low, the the total beam power does NOT exceed the 175 W limitation of the faraday cup.  The beam_stop, however is set at -0.1, which
#    presumably means beam blocker is OUT. So it may be that MyaPlot sometimes has a non-sensical value for certain PVs (e.g., 'beam_stop_atten')

# ------CY May 19 : Added rgm attenuation factors             ~   start_time(1st run)   end _time(last run)    Mya_beam_stop  Mya_beam_stop_atten  #  mya2ccdb.py Output Errors ??
# 6 GeV (more precisely, 5986.36 MeV)
#_ATTEN[5986]=15.3412   # Run range: 15016-15042, Eb=6 GeV, LH2    2021-11-10_18:15:38   2021-11-12_11:19:40                                    # mya2ccdb.py runs fine 
#_ATTEN[5986]=15.022    # Run range: 15043-15106, Eb=6 GeV, LD2    2021-11-12_17:45:21   2021-11-18_10:37:53    65.732          15.3412          # ERROR: File "mya2ccdb.py", line 233, in <module> data=dict(offset,**atten)  TypeError: type object argument after ** must be a mapping, not NoneType
#                                                                                                         (small dip to -0.1 
#                                                                                                           @ 11-16 16:55)
#_ATTEN[5986]=14.8788   # Run range: 15108-15164, Eb=6 GeV, LHe4   2021-11-18_17:57:19   2021-11-23_05:24:50   65.732            15.3412->14.8788      # mya2ccdb.py runs fine 
#                                                                                                          (small dip to -0.1   (@ 11-19 ~ 9:05 )
#                                                                                                            @11-18 18:52)
#_ATTEN[5986]=15.95795  # Run range: 15165-15177, Eb=6 GeV, Empty  2021-11-23_05:49:35   2021-11-23_19:04:56                                           # mya2ccdb.py runs fine 
#_ATTEN[5986]=12.017    # Run range: 15178-15317, Eb=6 GeV, C(x4)  2021-11-25_01:24:37   2021-12-05_08:04:44                                           # mya2ccdb.py runs fine 

#_ATTEN[5986]=8.183     # Run range: 15318-15328, Eb=6 GeV, Sn     2021-12-05_08:33:38   2021-12-06_07:50:47    65.732             12.017-> 8.181      # ERROR: File "mya2ccdb.py", line 233, in <module> data=dict(offset,**atten)  TypeError: type object argument after ** must be a mapping, not NoneType
#                                                                                                          (small dip to -0.1    (@ 12-05 15:18)
#                                                                                                       from ~ 12-05 09:59
#                                                                                                        to  ~ 12-5  10:14)

#_ATTEN[5986]=9.5178    # Run range: 15355-15389, Eb=6 GeV, 48Ca   2021-12-06_20:02:11   2021-12-10_07:30:18                                    # mya2ccdb.py runs fine 
#_ATTEN[5986]=12.6204   # Run range: 15390-15432, Eb=6 GeV, 40Ca   2021-12-10_17:23:13   2021-12-14_07:35:07                                    # mya2ccdb.py runs fine 
#_ATTEN[5986]=14.1515   # Run range: 15433-15456, Eb=6 GeV, LD2    2021-12-14_14:22:22   2021-12-17_07:30:42                                    # mya2ccdb.py runs fine 
#_ATTEN[5986]=13.9358   # Run range: 15458-15490, Eb=6 GeV, LHe4   2021-12-17_11:33:01   2021-12-21_07:19:07                                    # mya2ccdb.py runs fine 

# ------CY May 19 : Added rgm attenuation factors             ~   start_time(1st run)   end _time(last run)     Mya_beam_stop  Mya_beam_stop_atten
# 2.1 GeV (more precisely, 2070.52 MeV)
#_ATTEN[2070]=1.00000   # Run range: 15533-15565, Eb=2.1 GeV, LH2    2022-01-09_18:00:04   2022-01-13_08:01:42    65.732->-0.1   13.9358         # mya2ccdb.py runs fine
#                                                                                                               (@ 1-11 08:56)
#_ATTEN[2070]=1.00000   # Run range: 15566-15627, Eb=2.1 GeV, LD2    2022-01-13_12:28:32   2022-01-16_17:57:42                                  # ERROR: File "mya2ccdb.py", line 233, in <module> data=dict(offset,**atten)  TypeError: type object argument after ** must be a mapping, not NoneType
#_ATTEN[2070]=1.00000   # Run range: 15628-15636, Eb=2.1 GeV, LH2    2022-01-16_23:16:03   2022-01-17_11:27:56                                  # mya2ccdb.py runs fine
#_ATTEN[2070]=23.3452   # Run range: 15637-15642, Eb=2.1 GeV, Empty  2022-01-17_14:04:29   2022-01-18_13:26:35   53             13.9358          # mya2ccdb.py runs fine
#_ATTEN[2070]=1.00000   # Run range: 15643-15670, Eb=2.1 GeV, C      2022-01-18_23:43:07   2022-01-20_20:28:08  -0.1            13.9358         # mya2ccdb.py runs fine
#_ATTEN[2070]=1.00000   # Run range: 15671-15725, Eb=2.1 GeV, LAr    2022-01-20_20:32:28   2022-01-24_07:19:16  -0.1            13.9358         # mya2ccdb.py runs fine

# ------CY May 19 : Added rgm attenuation factors             ~   start_time(1st run)   end _time(last run)
# 4 GeV (more precisely, 4029.62 MeV), 
#_ATTEN[4029]=16.40835  # Run range: 15728-15732, Eb=4 GeV, Empty  2022-01-24_11:31:11  2022-01-24_12:15:05      53                  13.9358          # mya2ccdb.py runs fine
#_ATTEN[4029]=11.6961   # Run range: 15733,       Eb=4 GeV, C      2022-01-24_17:09:54  2022-01-24_18:15:47      53->-0.1 (@ 18:00)  13.9358          # ERROR:  File "mya2ccdb.py", line 198, in <module> offsets[len(offsets)-1].runMax=None IndexError: list index out of range
#_ATTEN[4029]=4.2662    # Run range: 15734,       Eb=4 GeV, LAr    2022-01-24_18:23:22  2022-01-24_22:20:02      53                  13.9358          # ERROR:  File "mya2ccdb.py", line 198, in <module> offsets[len(offsets)-1].runMax=None IndexError: list index out of range
#_ATTEN[4029]=16.40835  # Run range: 15735-15738, Eb=4 GeV, Empty  2022-01-25_18:00:16  2022-01-25_18:28:45      53                  13.9358          # mya2ccdb.py runs fine

#_ATTEN[4029]=4.2662    # Run range: 15739-15765, Eb=4 GeV, LAr    2022-01-27_14:19:02  2022-01-29_22:30:17      53->-0.1 (@ 20:37)  11.6961          # mya2ccdb.py runs fine
#                                                                                                              -0.1->53 (@ 21:59)

#_ATTEN[4029]=11.6961   # Run range: 15766-15775, Eb=4 GeV, C      2022-01-29_22:42:08  2022-01-30_13:30:35      53                  11.6961          # mya2ccdb.py runs fine
#_ATTEN[4029]=16.40835  # Run range: 15777,       Eb=4 GeV, Empty  2022-01-30_16:06:04  2022-01-30_18:04:04      53                  11.6961          # ERROR:  File "mya2ccdb.py", line 198, in <module> offsets[len(offsets)-1].runMax=None IndexError: list index out of range
#_ATTEN[4029]=11.6961   # Run range: 15778-15784, Eb=4 GeV, C      2022-01-30_18:10:51  2022-01-31_08:22:00      53                  11.6961           # mya2ccdb.py runs fine

# ------CY May 19 : Added rgm attenuation factors             ~   start_time(1st run)   end _time(last run)
# 6 GeV, (more precisely. 5986.36 MeV)
#_ATTEN[5986]=15.95795  # Run range: 15787-15788, Eb=6 GeV, Empty  2022-01-31_15:16:26   2022-01-31_15:46:50    -0.1->53 (@ 15:32)   11.6961          # mya2ccdb.py runs fine
#                                                                                                                53->-0.1 (@15:40)   

#_ATTEN[5986]=1.000000  # Run range: 15789-15802, Eb=6 GeV, LAr    2022-01-31_15:49:30   2022-02-01_19:58:52     -0.1                11.6961           # mya2ccdb.py runs fine
#_ATTEN[5986]=15.95795  # Run range: 15803,       Eb=6 GeV, Empty  2022-02-01_20:28:52   2022-02-01_21:28:59      53                  11.6961          # ERROR:  File "mya2ccdb.py", line 198, in <module> offsets[len(offsets)-1].runMax=None IndexError: list index out of range            

#_ATTEN[5986]=10.1932   # Run range: 15804-15827, Eb=6 GeV, Sn     2022-02-01_22:02:25   2022-02-03_07:30:43      53                 8.183->10.1932 (@ 14:17)     # mya2ccdb.py runs fine
#                                                                                                             (small dip to 
#                                                                                                             -0.1 @ 11:39)

_ATTEN[5986]=11.5568   # Run range: 15829-15884, Eb=6 GeV, 48Ca   2022-02-03_17:47:17   2022-02-08_06:00:55    53           10.1932->11.5568 (@21:51)            # mya2ccdb.py runs fine
#                                                                                                             (small dip to
#                                                                                                             -0.1 @21:15)




#_ATTEN[10604]= 9.8088
#_ATTEN[10409]= 9.6930
#_ATTEN[10405]= 9.6930  # unmeasured during BONuS, copied from 10409
#_ATTEN[10375]= 9.6930  # bogus beam energy from ACC during BONuS
#_ATTEN[10339]= 9.6930  # unmeasured during BONuS, copied from 10409
#_ATTEN[10389]= 9.6930  # unmeasured during BONuS, copied from 10409
#_ATTEN[10197]= 9.6930  # bogus beam energy from ACC, actually 10339
#_ATTEN[10200]= 9.96025
#_ATTEN[ 7546]=14.89565
#_ATTEN[ 6535]=16.283
#_ATTEN[ 6423]=16.9726

_OVERRIDE_ENERGY={
    RunRange(12444,12853,None):10405
}

class MyaFcup:
  def __init__(self,myaDatum):
    if not isinstance(myaDatum,MyaDatum):
      sys.exit('MyaFcup requires a MyaDatum')
    self.date = myaDatum.date
    self.time = myaDatum.time
    self.slm_atten = 1.0
    try:
      self.run = int(myaDatum.getValue('B_DAQ:run_number'))
      if self.run > _RUN_NUMBER_MAX:
        self.run=None
    except ValueError:
      self.run = None
    try:
      self.energy = float(myaDatum.getValue('MBSY2C_energy'))
    except ValueError:
      self.energy = None
    try:
      # convert to -1/0/+1=IN/UDF/OUT scheme:
      self.hwp = int(myaDatum.getValue('IGL1I00OD16_16'))
      if   self.hwp == 1: self.hwp = -1
      elif self.hwp == 0: self.hwp = 1
      else:               self.hwp = 0
    except ValueError:
      self.hwp = None
    try:
      self.offset = float(myaDatum.getValue('fcup_offset'))
    except ValueError:
      self.offset = None
    try:
      self.slm_offset = float(myaDatum.getValue('slm_offset'))
    except ValueError:
      self.slm_offset = None
    try:
      self.stopper = float(myaDatum.getValue('beam_stop'))
    except ValueError:
      self.stopper = None
    self.energy = self.correctEnergy()
    self.atten = self.getAttenuation()
  def correctEnergy(self):
    if self.energy is not None:
      for runRange,energy in _OVERRIDE_ENERGY.items():
        if runRange.contains(self.run):
          return energy
    return self.energy
  def getAttenuation(self):
    if self.energy is None:
      return None
    if self.stopper is None:
      return None
    if self.stopper < _BEAM_STOP_THRESHOLD:
      return 1.0
    for e in _ATTEN.keys():
      if abs(e-self.energy) < _BEAM_ENERGY_TOLERANCE:
        return _ATTEN[e]
    return None
  def __str__(self):
    s='%10s %10s'%(self.date,self.time)
    if self.run is None:     s+=' %5s'%self.run
    else:                    s+=' %5d'%self.run
    if self.energy is None:  s+=' %8s'%self.energy
    else:                    s+=' %8.2f'%self.energy
    if self.stopper is None: s+=' %6s'%self.stopper
    else:                    s+=' %6.3f'%self.stopper
    if self.atten is None:   s+=' %8s'%self.atten
    else:                    s+=' %8.5f'%self.atten
    if self.offset is None:  s+=' %6s'%self.offset
    else:                    s+=' %5.1f'%self.offset
    s+=' %s'%self.hwp
    return s

