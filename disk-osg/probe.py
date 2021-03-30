#!/usr/bin/env python3

import re
import os
import sys
import glob
import json
import argparse
import subprocess

json_format =  {'indent':2, 'separators':(',',': '), 'sort_keys':True}
log_regex = '/([a-z]+)/job_([0-9]+)/log/job\.([0-9]+)\.([0-9]+)\.'

condor_data = None

def condor_query(cluster_id):
  '''Get the JSON for a particular job from condor_q
  cluster_id must be fully qualified, e.g. #####.##'''
  global condor_data
  if condor_data is None:
    cmd = ['condor_q','gemc','-nobatch','-json']
    condor_data = json.loads(subprocess.check_output(cmd).decode('UTF-8'))
  for x in condor_data:
    if 'ClusterId' in x and 'ProcId' in x:
      if '%d.%d' % (x['ClusterId'],x['ProcId']) == cluster_id:
        return x
  return None

def readlines_reverse(filename,max_lines):
  '''Get the trailing lines from a file, stopping
  after max_lines unless max_lines is negative'''
  n_lines = 0
  with open(filename) as qfile:
    qfile.seek(0, os.SEEK_END)
    position = qfile.tell()
    line = ''
    while position >= 0:
      if n_lines > max_lines and max_lines>0:
        break
      qfile.seek(position)
      try:
        next_char = qfile.read(1)
      except:
        next_char = ''
      if next_char == "\n":
         n_lines += 1
         yield line[::-1]
         line = ''
      else:
         line += next_char
      position -= 1
  yield line[::-1]

def crawl():
  '''Crawl the log directory, linking condor/gemc
  job ids, user names, and log files'''
  ret = {}
  for dirpath,dirnames,filenames in os.walk('/osgpool/hallb/clas12/gemc'):
    for filename in filenames:
      fullfilepath = dirpath+'/'+filename
      m = re.search(log_regex,fullfilepath)
      if m is None:
        continue
      user = m.group(1)
      gemc = m.group(2)
      condor = m.group(3)+'.'+m.group(4)
      if condor not in ret:
        ret[condor] = {'gemc':gemc, 'user':user, 'logs':[]}
      ret[condor]['logs'].append(fullfilepath)
  return ret

if __name__ == '__main__':

  cli = argparse.ArgumentParser()
  cli.add_argument('-condor', default=[], metavar='# or #.#', action='append', type=str, help='limit by condor cluster id')
  cli.add_argument('-gemc', default=[], metavar='#', action='append', type=str, help='limit by gemc job id')
  cli.add_argument('-user', default=[], action='append', type=str, help='limit by user name')
  cli.add_argument('-held', default=False, action='store_true', help='limit to jobs currently in held state')
  cli.add_argument('-running', default=False, action='store_true', help='limit to jobs currently in running state')
  cli.add_argument('-tail', default=None, metavar='#', type=int, help='dump last # lines of logs (all=negative, 0=just-names)')
  cli.add_argument('-json', default=False, action='store_true', help='dump JSON')

  args = cli.parse_args(sys.argv[1:])

  for condor,val in sorted(crawl().items()):

    if len(args.condor) > 0:
      if condor not in args.condor:
        if condor.split('.').pop(0) not in args.condor:
          continue
 
    if len(args.gemc) > 0 and val['gemc'] not in args.gemc:
      continue
 
    if len(args.user) > 0 and val['user'] not in args.user:
      continue

    if args.held or args.running:
      if condor_query(condor) is None:
        continue
      if args.held and condor_query(condor).get('JobStatus') != 5:
        continue 
      if args.running and condor_query(condor).get('JobStatus') != 2:
        continue
 
    print('%16s %10s %12s' % (condor,val['gemc'],val['user']))

    if args.json:
      print(json.dumps(condor_query(condor),**json_format))

    if args.tail is not None:
      for x in val['logs']:
        print(x)
        if args.tail != 0:
          print('\n'.join(readlines_reverse(x, args.tail)))
          print()




# Example of "condor_q -nobatch -json":
#{
#  "Args": "2615 81",
#  "AutoClusterAttrs": "_condor_RequestCpus,_condor_RequestDisk,_condor_RequestGPUs,_condor_RequestMemory,_cp_orig_RequestCpus,_cp_orig_RequestDisk,_cp_orig_RequestMemory,CVMFS_stash_osgstorage_org_REVISION,DESIRED_Sites,DynamicSlot,HasJava,IsCOVID19,ITB_Factory,ITB_Sites,JobUniverse,LastCheckpointPlatform,LastHeardFrom,MachineLastMatchTime,Memory,NumCkpts,OSG_NODE_VALIDATED,PartitionableSlot,ProjectName,QDate,Rank,RemoteOwner,RequestCpus,RequestDisk,RequestGPUs,RequestMemory,RunOnSubmitNode,SleepSlot,Slot10_RemoteOwner,Slot1_RemoteOwner,Slot1_SelfMonitorAge,Slot1_TotalTimeClaimedBusy,Slot1_TotalTimeUnclaimedIdle,Slot2_RemoteOwner,Slot3_RemoteOwner,Slot4_RemoteOwner,Slot5_RemoteOwner,Slot6_RemoteOwner,Slot7_RemoteOwner,Slot8_RemoteOwner,Slot9_RemoteOwner,SUBMITTER_stash_osgstorage_org_REVISION,TotalJobRuntime,undeined,UNDESIRED_Sites,User,WantsStashCvmfs,XENON_DESIRED_Sites,ConcurrencyLimits,NiceUser,Requirements,flock,x509userproxysubject,D_PID,is_itb,WantsXSEDE,WantTigerBackfill",
#  "AutoClusterId": 34046,
#  "BlockReadKbytes": 0,
#  "BlockReads": 0,
#  "BlockWriteKbytes": 0,
#  "BlockWrites": 0,
#  "BufferBlockSize": 32768,
#  "BufferSize": 524288,
#  "BytesRecvd": 29308.0,
#  "BytesSent": 5828350.0,
#  "ClusterId": 3472942,
#  "Cmd": "/osgpool/hallb/clas12/gemc/jnewton/job_2615/run.sh",
#  "CommittedSlotTime": 0,
#  "CommittedSuspensionTime": 0,
#  "CommittedTime": 0,
#  "CompletionDate": 0,
#  "CondorPlatform": "$CondorPlatform: X86_64-CentOS_7.8 $",
#  "CondorVersion": "$CondorVersion: 8.8.10 Aug 17 2020 PackageID: 8.8.10-1.1 $",
#  "CoreSize": 0,
#  "CpusProvisioned": 1,
#  "CPUsUsage": 0.9974730512006985,
#  "CumulativeRemoteSysCpu": 347.0,
#                                                                                                                                                      907,3          2%
#  "CPUsUsage": 0.9974730512006985,
#  "CumulativeRemoteSysCpu": 347.0,
#  "CumulativeRemoteUserCpu": 38388.0,
#  "CumulativeSlotTime": 37327.0,
#  "CumulativeSuspensionTime": 0,
#  "CurrentHosts": 1,
#  "DiskProvisioned": 4580568,
#  "DiskUsage": 750000,
#  "DiskUsage_RAW": 629829,
#  "EncryptExecuteDirectory": false,
#  "EnteredCurrentStatus": 1617115471,
#  "Environment": "",
#  "Err": "log/job.3472942.81.err",
#  "ExecutableSize": 1,
#  "ExecutableSize_RAW": 1,
#  "ExitBySignal": false,
#  "ExitStatus": 0,
#  "GlobalJobId": "scosg16.jlab.org#3472942.81#1616890323",
#  "ImageSize": 7500000,
#  "ImageSize_RAW": 5241464,
#  "In": "/dev/null",
#  "Iwd": "/osgpool/hallb/clas12/gemc/jnewton/job_2615",
#  "JOB_GLIDEIN_ClusterId": "$$(GLIDEIN_ClusterId:Unknown)",
#  "JOB_GLIDEIN_Entry_Name": "$$(GLIDEIN_Entry_Name:Unknown)",
#  "JOB_GLIDEIN_Factory": "$$(GLIDEIN_Factory:Unknown)",
#  "JOB_GLIDEIN_Name": "$$(GLIDEIN_Name:Unknown)",
#  "JOB_GLIDEIN_ProcId": "$$(GLIDEIN_ProcId:Unknown)",
#  "JOB_GLIDEIN_Schedd": "$$(GLIDEIN_Schedd:Unknown)",
#  "JOB_GLIDEIN_Site": "$$(GLIDEIN_Site:Unknown)",
#  "JOB_GLIDEIN_SiteWMS": "$$(GLIDEIN_SiteWMS:Unknown)",
#  "JOB_GLIDEIN_SiteWMS_JobId": "$$(GLIDEIN_SiteWMS_JobId:Unknown)",
#  "JOB_GLIDEIN_SiteWMS_Queue": "$$(GLIDEIN_SiteWMS_Queue:Unknown)",
#  "JOB_GLIDEIN_SiteWMS_Slot": "$$(GLIDEIN_SiteWMS_Slot:Unknown)",
#  "JOB_Site": "$$(GLIDEIN_Site:Unknown)",
#  "JobAdInformationAttrs": "JOB_Site JOB_GLIDEIN_Entry_Name JOB_GLIDEIN_Name JOB_GLIDEIN_Factory JOB_GLIDEIN_Schedd JOB_GLIDEIN_ClusterId JOB_GLIDEIN_ProcId JOB_GLIDEIN_Site JOB_GLIDEIN_SiteWMS JOB_GLIDEIN_SiteWMS_Slot JOB_GLIDEIN_SiteWMS_JobId JOB_GLIDEIN_SiteWMS_Queue",
#  "JobCurrentStartDate": 1617115471,
#  "JobCurrentStartExecutingDate": 1617115471,
#  "JobCurrentStartTransferOutputDate": 1616927887,
#  "JOBGLIDEIN_ResourceName": "$$([IfThenElse(IsUndefined(TARGET.GLIDEIN_ResourceName), IfThenElse(IsUndefined(TARGET.GLIDEIN_Site), \"Local Job\", TARGET.GLIDEIN_Site), TARGET.GLIDEIN_ResourceName)])",
#  "JobLastStartDate": 1616927843,
#  "JobLeaseDuration": 2400,
#  "JobNotification": 0,
#  "JobPrio": 0,
#  "JobRunCount": 4,
#  "JobStartDate": 1616890494,
#  "JobStatus": 2,
#                                                                                                                                                      948,3          2%
#  "JobStartDate": 1616890494,
#  "JobStatus": 2,
#  "JobUniverse": 5,
#  "LastHoldReason": "The job attribute OnExitHold expression '(ExitBySignal == true) || (ExitCode != 0)' evaluated to TRUE",
#  "LastHoldReasonCode": 3,
#  "LastHoldReasonSubCode": 0,
#  "LastJobLeaseRenewal": 1617117385,
#  "LastJobStatus": 1,
#  "LastMatchTime": 1617115471,
#  "LastPublicClaimId": "<10.242.15.9:44058?CCBID=192.170.227.251:9796%3faddrs%3d192.170.227.251-9796+[2605-9a00-10-400d-7686-7aff-fedd-d118]-9796%26alias%3dflock.opensciencegrid.org#1354213&PrivNet=notch009.int.chpc.utah.edu&addrs=10.242.15.9-44058&alias=notch009.int.chpc.utah.edu&noUDP>#1616916816#148#...",
#  "LastRejMatchReason": "no match found ",
#  "LastRejMatchTime": 1616908813,
#  "LastRemoteHost": "slot1_6@glidein_99879_43933758@notch009.int.chpc.utah.edu",
#  "LastRemotePool": "flock.opensciencegrid.org",
#  "LastSuspensionTime": 0,
#  "LastVacateTime": 1616927815,
#  "LeaveJobInQueue": false,
#  "LocalSysCpu": 0.0,
#  "LocalUserCpu": 0.0,
#  "MachineAttrCpus0": 1,
#  "MachineAttrSlotWeight0": 1,
#  "MATCH_EXP_JOB_GLIDEIN_ClusterId": "5552907",
#  "MATCH_EXP_JOB_GLIDEIN_Entry_Name": "Glow_US_Syracuse_condor-ce2",
#  "MATCH_EXP_JOB_GLIDEIN_Factory": "OSG",
#  "MATCH_EXP_JOB_GLIDEIN_Name": "gfactory_instance",
#  "MATCH_EXP_JOB_GLIDEIN_ProcId": "2",
#  "MATCH_EXP_JOB_GLIDEIN_Schedd": "schedd_glideins8@gfactory-2.opensciencegrid.org",
#  "MATCH_EXP_JOB_GLIDEIN_Site": "SU-ITS",
#  "MATCH_EXP_JOB_GLIDEIN_SiteWMS": "HTCondor",
#  "MATCH_EXP_JOB_GLIDEIN_SiteWMS_JobId": "13517371.0",
#  "MATCH_EXP_JOB_GLIDEIN_SiteWMS_Queue": "its-condor-ce2.syr.edu",
#  "MATCH_EXP_JOB_GLIDEIN_SiteWMS_Slot": "slot26@CRUSH-OSG-C7-10-5-219-52",
#  "MATCH_EXP_JOB_Site": "SU-ITS",
#  "MATCH_EXP_JOBGLIDEIN_ResourceName": "SU-ITS-CE2",
#  "MATCH_GLIDEIN_ClusterId": 5552907,
#  "MATCH_GLIDEIN_Entry_Name": "Glow_US_Syracuse_condor-ce2",
#  "MATCH_GLIDEIN_Factory": "OSG",
#  "MATCH_GLIDEIN_Name": "gfactory_instance",
#  "MATCH_GLIDEIN_ProcId": 2,
#  "MATCH_GLIDEIN_Schedd": "schedd_glideins8@gfactory-2.opensciencegrid.org",
#  "MATCH_GLIDEIN_Site": "SU-ITS",
#  "MATCH_GLIDEIN_SiteWMS": "HTCondor",
#  "MATCH_GLIDEIN_SiteWMS_JobId": "13517371.0",
#  "MATCH_GLIDEIN_SiteWMS_Queue": "its-condor-ce2.syr.edu",
#  "MATCH_GLIDEIN_SiteWMS_Slot": "slot26@CRUSH-OSG-C7-10-5-219-52",
#  "MaxHosts": 1,
#  "MemoryProvisioned": 2500,
#                                                                                                                                                      992,3          3%
#  "MaxHosts": 1,
#  "MemoryProvisioned": 2500,
#  "MemoryUsage": "\/Expr(((ResidentSetSize + 1023) / 1024))\/",
#  "MinHosts": 1,
#  "MyType": "Job",
#  "NiceUser": false,
#  "NumCkpts": 0,
#  "NumCkpts_RAW": 0,
#  "NumJobCompletions": 1,
#  "NumJobMatches": 4,
#  "NumJobStarts": 4,
#  "NumRestarts": 0,
#  "NumShadowStarts": 4,
#  "NumSystemHolds": 0,
#  "OnExitHold": "\/Expr((ExitBySignal == true) || (ExitCode != 0))\/",
#  "OnExitRemove": "\/Expr((ExitBySignal == false) && (ExitCode == 0))\/",
#  "OrigMaxHosts": 1,
#  "Out": "log/job.3472942.81.out",
#  "Owner": "gemc",
#  "PeriodicHold": false,
#  "PeriodicRelease": "\/Expr((NumJobStarts < 3) && ((CurrentTime - EnteredCurrentStatus) > (60 * 60)))\/",
#  "PeriodicRemove": false,
#  "ProcId": 81,
#  "ProjectName": "CLAS12",
#  "PublicClaimId": "<10.5.219.52:35401?CCBID=192.170.227.251:9708%3faddrs%3d192.170.227.251-9708+[2605-9a00-10-400d-7686-7aff-fedd-d118]-9708%26alias%3dflock.opensciencegrid.org#1377596&PrivNet=CRUSH-OSG-C7-10-5-219-52&addrs=10.5.219.52-35401&alias=CRUSH-OSG-C7-10-5-219-52&noUDP>#1617111883#1#...",
#  "QDate": 1616890323,
#  "Rank": 0.0,
#  "RecentBlockReadKbytes": 0,
#  "RecentBlockReads": 0,
#  "RecentBlockWriteKbytes": 0,
#  "RecentBlockWrites": 0,
#  "RecentStatsLifetimeStarter": 1200,
#  "ReleaseReason": "via condor_release (by user gemc)",
#  "RemoteHost": "glidein_11052_156649344@CRUSH-OSG-C7-10-5-219-52",
#  "RemotePool": "flock.opensciencegrid.org",
#  "RemoteSlotID": 1,
#  "RemoteSysCpu": 19.0,
#  "RemoteUserCpu": 1452.0,
#  "RemoteWallClockTime": 37327.0,
#  "RequestCpus": 1,
#  "RequestDisk": 1048576,
#  "RequestMemory": 1741,
#  "Requirements": "\/Expr(((HAS_SINGULARITY is true) && (HAS_CVMFS_oasis_opensciencegrid_org is true)) && (TARGET.Arch == \"X86_64\") && (TARGET.OpSys == \"LINUX\") && (TARGET.Disk >= RequestDisk) && (TARGET.Memory >= RequestMemory) && (TARGET.HasFileTransfer))\/",
#  "ResidentSetSize": 1750000,
#  "ResidentSetSize_RAW": 1551120,
#  "RootDir": "/",
#                                                                                                                                                      1037,3         3%
#  "ResidentSetSize_RAW": 1551120,
#  "RootDir": "/",
#  "ServerTime": 1617117434,
#  "ShadowBday": 1617115471,
#  "ShouldTransferFiles": "YES",
#  "SingularityBindCVMFS": true,
#  "SingularityImage": "\/Expr('/cvmfs/singularity.opensciencegrid.org/jeffersonlab/clas12software:production')\/",
#  "StartdIpAddr": "<10.5.219.52:35401?CCBID=192.170.227.251:9708%3faddrs%3d192.170.227.251-9708+[2605-9a00-10-400d-7686-7aff-fedd-d118]-9708%26alias%3dflock.opensciencegrid.org#1377596&PrivNet=CRUSH-OSG-C7-10-5-219-52&addrs=10.5.219.52-35401&alias=CRUSH-OSG-C7-10-5-219-52&noUDP>",
#  "StartdPrincipal": "execute-side@matchsession/128.230.11.10",
#  "StatsLifetimeStarter": 1511,
#  "StreamErr": false,
#  "StreamOut": false,
#  "TargetType": "Machine",
#  "TotalSubmitProcs": 100,
#  "TotalSuspensions": 0,
#  "TransferIn": false,
#  "TransferInput": "run.sh,nodeScript.sh",
#  "TransferInputSizeMB": 0,
#  "TransferOutput": "output",
#  "TransferQueued": false,
#  "TransferringInput": false,
#  "User": "gemc@scosg16.jlab.org",
#  "UserLog": "/osgpool/hallb/clas12/gemc/jnewton/job_2615/log/job.3472942.81.log",
#  "WantCheckpoint": false,
#  "WantRemoteIO": true,
#  "WantRemoteSyscalls": false,
#  "WhenToTransferOutput": "ON_EXIT"
#}
