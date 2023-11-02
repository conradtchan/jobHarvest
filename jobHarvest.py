#!/usr/bin/python3 -u

# read OST/MDT lustre job_stats
#
# 'clients' - send from lustre servers synchronised dumps of current json job_stats.
# 'server'  - simplifies and accumulates the json into a running total of
#               jobid, {fs's}, {mdt iops, ost iops, ost read, ost write}
#
#     intermediate steps in the server code are
#       - keep all i/o based on
#           {ost or mdt}, jobid, {mdt iops or ost iops, ost read, ost write}
#         each with timestamps
#       - handle the case where various {ost,mdt} don't talk to jobid for a
#         while (typically >600s), and so the jobstats disappear. if/when they come back
#         they reset to zero. so we need to track comings and goings and timestamps.
#
#   (c) Robin Humble - Thu May  5 18:31:06 AEST 2022

# read lustre OSTs eg.
#   /proc/fs/lustre/obdfilter/dagg-OST0000/job_stats
# or MDTs eg.
#   /proc/fs/lustre/mdt/dagg-MDT0001/job_stats

import os, socket, select, sys, pickle, time, hashlib, json, copy

port = 8023  # default port
clientSend = 3  # number of gathers per minute on clients
serverInterfaceName = None

# job_stats file locations on oss/mds. handle v1.8 and 2.5
statsDir = { 'oss':['/proc/fs/lustre/obdfilter'], 'mds':['/proc/fs/lustre/mds', '/proc/fs/lustre/mdt'] }

# lustre jobs_stats expiry time, as configured in lustre
jobStatsExpiry = 600

verbose = 0
dryrun = 0

# shared secret between client and servers. only readable by root
secretFile = '/root/.lustreHarvest.secret'

# influx credentials file (if file doesn't exist, influx is disabled):
influxConfigFile = '/root/.jobHarvest.influx'
# Template:
#
# http://server_address:port
# org_name
# bucket_name
# token
influxConfig = None

dt = 60.0/clientSend
secretText = None

def readSecret():
   global secretText
   try:
      l = open( secretFile, 'r' ).readlines()
   except:
      print('problem reading secret file:', secretFile, file=sys.stderr)
      sys.exit(1)
   # check for and complain about an empty secret file
   ok = 0
   for i in l:
      i = i.strip()
      if i != '':
         ok = 1
         break
   if not ok:
      print('nothing in the shared secret file:', secretFile, file=sys.stderr)
      sys.exit(1)
   secretText = str(l)

def gatherStats(fs):
   s = {}
   osts = []
   # handle both mds and oss
   for machType, lld in statsDir.items():
      for ld in lld:  # loop over possible stats dirs looking for fsName-*
         try:
            dirs = os.listdir(ld)
         except:
            continue
         # find osts
         for d in dirs:
            if d[:len(fs)] == fs and len(d) > len(fs) and d[len(fs)] == '-':
               osts.append((machType, ld, d))

   if verbose:
      print('osts', osts)

   for machType, ld, o in osts:
      s[o] = {}
      s[o]['type'] = machType   # oss or mds data
      fn = ld + '/' + o + '/job_stats'
      with open(fn, 'r') as f:
         s[o]['data'] = f.read()

   return s

def uniq( list ):
   l = []
   prev = None
   for i in list:
      if i != prev:
         l.append( i )
      prev = i
   return l

def zeroOss(o):
   o['size'] = -1
   # leave o['data'] intact

def removeProcessedData(o):
   for oss in list(o.keys()):
      if 'data' in list(o[oss].keys()):
         o[oss]['data'] = {}

def parseJob(j):
   # eg.
   # - job_id:          27161099
   #   snapshot_time:   1652881026
   #   read_bytes:      { samples:           0, unit: bytes, min:       0, max:       0, sum:               0 }
   #   write_bytes:     { samples:       30312, unit: bytes, min:     520, max:    6733, sum:        99963674 }
   #   getattr:         { samples:           0, unit:  reqs }
   #   setattr:         { samples:           0, unit:  reqs }
   #   punch:           { samples:           5, unit:  reqs }
   #   sync:            { samples:           0, unit:  reqs }
   #    ...
   #   quotactl:        { samples:           0, unit:  reqs }
   d = {}
   iops = 0
   for i in j:
      #print('i', i)
      if i[0] == '-':
         #d['job_id'] = i[2]
         continue
      k = i[0][:-1]  # strip off the trailing :
      if k == 'snapshot_time':
         v = int(i[1])
      elif k == 'read_bytes' or k == 'write_bytes':
         v = int(i[11])  # sum
      else:
         v = int(i[3][:-1])   # strip off trailing comma
         iops += v
      d[k] = v
   d['iops'] = iops
   #print(d)
   #print('job,r,w,i:', d['job_id'], d['read_bytes'], d['write_bytes'], d['iops'])
   #print('job', d['job_id'])

   return d

def parseOss(o):
   for oss in list(o.keys()):
      #print('oss', oss)
      #print('o[oss].keys()', o[oss].keys())
      #if 'data' in list(o[oss].keys()):
      #   print('o[oss][\'data\'].keys()', o[oss]['data'].keys())
      for fs in o[oss]['data'].keys():
         #print('o[oss]["data"]["' + fs + '"].keys()', o[oss]['data'][fs].keys())
         for ost in o[oss]['data'][fs].keys():
            #print('ost', ost, 'o[oss]["data"][fs][ost].keys()', o[oss]['data'][fs][ost].keys())
            #print('   type', o[oss]['data'][fs][ost]['type'], 'len(data)', len(o[oss]['data'][fs][ost]['data']))
            ##for j in o[oss]['data'][fs][ost].keys():
            ##   print('   ', j, o[oss]['data'][fs][ost][j])
            p = {}
            o[oss]['data'][fs][ost]['parsed'] = p
            d = o[oss]['data'][fs][ost]['data']
            j = []
            job = None
            # eg.
            # job_stats:
            # - job_id:          27161099
            #    ...
            # - job_id:          27161091
            #    ...
            #
            dd = d.split('\n')
            if len(dd) <= 2:
               # corner case: un-accessed ost has just the first line and nothing else ie.
               #   job_stats:
               continue

            for i in dd:
               #print('i',i)
               ii = i.strip().split()
               if len(ii) > 0 and ii[0] == 'job_stats:':  # skip the first line
                  continue
               if len(ii) == 0:  # EOF. process final job
                  p[job] = parseJob(j)
                  break
               if ii[0] == '-':  # new job field. process the previous job's data
                  if job != None:   # skip the first - job_id:
                     p[job] = parseJob(j)
                  if len(ii) == 3:
                     job = ii[2]
                  else:
                     job = 'unknown'
                  #print('job', job)
                  j = []
               j.append(ii)

def rearrangeByTgt(o, tmeta):
   t = {}
   for oss in list(o.keys()):
      for fs in o[oss]['data'].keys():
         for ost in o[oss]['data'][fs].keys():
            if ost in t.keys():
               print('error: target', ost, 'already in t', file=sys.stderr)
            t[ost] = o[oss]['data'][fs][ost]['parsed']
            # update tmeta if necessary
            if ost not in tmeta.keys():
               tmeta[ost] = {}
               tmeta[ost]['type'] = o[oss]['data'][fs][ost]['type']
               tmeta[ost]['fs'] = fs
               tmeta[ost]['oss'] = oss
   return t

def stripOutNonjobs(t):
   nonjob = {}
   for tgt in t.keys():
      todel = []
      nonjob[tgt] = {}
      for j in t[tgt].keys():
         if j == None:  # dunno how this happens
            print('WTF. j,t[tgt][j]', j, t[tgt][j])
         if j == None or not j.isnumeric():
            todel.append(j)
            nonjob[tgt][j] = t[tgt][j]
      for j in todel:
         del t[tgt][j]
   return nonjob

def addFields(a, b):
   """c = a+b, except snapshot_time is from b"""
   c = {}
   assert(a.keys() == b.keys())
   for k,v in a.items():
      c[k] = v + b[k]
   c['snapshot_time'] = b['snapshot_time']
   return c

def addToTLong(g, tl):
   for tgt in g.keys():
      if tgt not in tl.keys():
         tl[tgt] = {}
      for j in g[tgt].keys():
         if j not in tl[tgt].keys():  # job wasn't in tlong
            tl[tgt][j] = g[tgt][j]
         else:
            assert(tl[tgt][j]['snapshot_time'] <= g[tgt][j]['snapshot_time'])
            tl[tgt][j] = addFields(tl[tgt][j], g[tgt][j])

def rateFields(a, b, inv_dt):
   """c = (a-b)*inv_dt, except snapshot_time is from a"""
   c = {}
   assert(a.keys() == b.keys())
   for k,v in a.items():
      c[k] = inv_dt*float(v - b[k])
   c['snapshot_time'] = a['snapshot_time']
   return c

def findGone(t, tPrev):
   gone = {}
   dtmax = 0
   dtmax_backwards = 0
   dtmax_nonbackwards = 0
   for tgt in tPrev.keys():
      if tgt not in t.keys():  # whole tgt has gone
         gone[tgt] = tPrev[tgt]
         continue
      gone[tgt] = {}
      for j in tPrev[tgt].keys():
         if j not in t[tgt].keys():
            gone[tgt][j] = tPrev[tgt][j]
         else:  # j in t and tPrev
            # however, data can also expire after 600s and then come back at ~601s before we sample it at ~630-660s. this is common.
            # in that case we get a timestamp that is >= 600s from the last one, and data that has (hopefully) gone -ve so that we can detect it for sure
            dt = t[tgt][j]['snapshot_time'] - tPrev[tgt][j]['snapshot_time']
            dtmax = max(dt, dtmax)

            # check for any value going backwards which would indicate: gone from tPrev & returned in t
            backwards = 0
            assert(t[tgt][j].keys() == tPrev[tgt][j].keys())
            for k, v in t[tgt][j].items():
               if k == 'snapshot_time': # ignore time, natch
                  continue
               if tPrev[tgt][j][k] > v:
                  backwards += 1

            if dt == 0:
               # nothing has changed
               continue

            if backwards and dt < jobStatsExpiry:
               if verbose:
                  print('error: j,tgt',j,tgt,'dt',dt,'backwards',backwards)

            if dt > jobStatsExpiry:   # backwards should always be accompanied by expired dt
               if backwards:
                  dtmax_backwards = max(dt, dtmax_backwards)
                  #print('j,tgt',j,tgt,'dt',dt,'backwards',backwards,'- setting gone')
                  gone[tgt][j] = tPrev[tgt][j]
               else:
                  dtmax_nonbackwards = max(dt, dtmax_nonbackwards)
                  if verbose:
                     print('warning: looks like gone/return, but no backwards: j,tgt',j,tgt,'dt',dt,'backwards',backwards)
                     print('   tPrev',tPrev[tgt][j])
                     print('       t',t[tgt][j])

   if verbose:
      print('dtmax', dtmax, 'dtmax backwards', dtmax_backwards, 'dtmax without going backwards', dtmax_nonbackwards)

   return gone

def sumJob(t, tLong, tmeta):
   jSum = {}
   for s in [ t, tLong ]:
      for tgt in s.keys():
         fs = tmeta[tgt]['fs']     # dagg for dagg-OST0001
         typ = tmeta[tgt]['type']  # ost or mdt
         for j in s[tgt].keys():
            if j not in jSum.keys():
               jSum[j] = {}
            if fs not in jSum[j].keys():
               jSum[j][fs] = {}
            if typ not in jSum[j][fs].keys():
               jSum[j][fs][typ] = copy.deepcopy(s[tgt][j])
            else:
               jSum[j][fs][typ] = addFields(jSum[j][fs][typ], s[tgt][j])
   return jSum

# eg. tSum = sumTTLong(t, tLong)
def sumTTLong(a, b):
   """c = sum(a,b) - a union, adding where they overlap."""
   c = copy.deepcopy(b)
   for tgt in a.keys():
      if tgt not in c:
         c[tgt] = copy.deepcopy(a[tgt])
         continue
      for j in a[tgt].keys():
         if j not in c[tgt].keys():
            c[tgt][j] = copy.deepcopy(a[tgt][j])  # job in a but not b
         else:
            c[tgt][j] = addFields(c[tgt][j], a[tgt][j])
   return c


def monotonic(a, prev):
   assert(a.keys() == prev.keys())
   up=0
   err=0
   for k,v in a.items():
      if k == 'snapshot_time':
         continue
      if v < prev[k]:
         err=1
      elif v > prev[k]:
         up=1
   return up, err

def checkDataVsTimestamps(a, prev):
   gone = {}
   for tgt in a:
      if tgt not in prev:
         continue
      for j in a[tgt].keys():
         if j not in prev[tgt].keys():
            continue
         ts_a = a[tgt][j]['snapshot_time']
         ts_prev = prev[tgt][j]['snapshot_time']
         assert(ts_a >= ts_prev)
         if ts_a > ts_prev:
            up, err = monotonic(a[tgt][j], prev[tgt][j])
            if not up or err:
               # data has stayed the same or gone down, but snapshot_time has gone up, so this must be new i/o
               if tgt not in gone.keys():
                  gone[tgt] = {}
               gone[tgt][j] = prev[tgt][j]

def jobRate(js, jsp, inv_dt):
   r = {}
   err = []
   for j in js.keys() & jsp.keys():
      r[j] = {}
      for fs in js[j].keys():
         if fs not in jsp[j].keys():
            continue
         if fs not in r[j].keys():
            r[j][fs] = {}
         for typ in js[j][fs].keys():
            if typ not in jsp[j][fs].keys():
               continue
            if typ not in r[j][fs].keys():
               r[j][fs][typ] = {}
            r[j][fs][typ] = rateFields(js[j][fs][typ], jsp[j][fs][typ], inv_dt)
            # none of these should ever be -ve
            for k,v in r[j][fs][typ].items():
               if v < 0.0:
                  print('err: -ve rate: %s %s %s %s %.1f' % (j,fs,typ,k,v))
                  err.append((j,fs,typ,k,v))
   return r, err

def printJobRates(r, high):
   jobs = {}
   for typ in high.keys():
      for i in high[typ].keys():
         for j in r.keys():
            for fs in r[j].keys():
               if typ in r[j][fs].keys() and r[j][fs][typ][i] > high[typ][i]:
                  if j not in jobs.keys():
                     jobs[j] = []
                  jobs[j].append((fs,typ,i))
   for j,lst in jobs.items():
      print(j, end='')
      for (fs,typ,i) in lst:
         print(' %s %s %s %.0f' % ( fs, typ, i, r[j][fs][typ][i] ), end='')
      print('')

def shortJobRates(r, high, allFs):
   """all fs's, only the fields that are in high, set to 0.0 if not in r"""
   s = {}
   for j in r.keys():
      s[j] = {}
      for fs in allFs:
         s[j][fs] = {}
         for typ in high.keys():
            s[j][fs][typ] = {}
            for i in high[typ].keys():
               s[j][fs][typ][i] = 0.0
               if fs in r[j].keys() and typ in r[j][fs].keys() and i in r[j][fs][typ].keys():
                  s[j][fs][typ][i] = r[j][fs][typ][i]
   return s

def superShortJobRates(r, high, allFs):   # @@@ need to add allFs code to the below
   """compact even more. sum together all fs's for each job"""
   s = {}
   for j in r.keys():
      s[j] = {}
      for fs in r[j].keys():
         for typ in high.keys():
            if typ in r[j][fs].keys():
               if typ not in s[j].keys():
                  s[j][typ] = {}
               for i in high[typ].keys():
                  if i in r[j][fs][typ].keys():
                     s[j][typ][i] = r[j][fs][typ][i]
   return s

def fsRate(r, high):
   fsSum = {}
   for j in r.keys():
      for fs in r[j].keys():
         if fs not in fsSum.keys():
            fsSum[fs] = {}
         for typ in r[j][fs].keys():
            if typ not in fsSum[fs].keys():
               fsSum[fs][typ] = copy.deepcopy(r[j][fs][typ])
            else:
               fsSum[fs][typ] = addFields(fsSum[fs][typ], r[j][fs][typ])
   #print('fs',fsSum)

   # print based on things we flag as interesting in 'high'
   for fs in fsSum.keys():
      if verbose:
         print('fs', fs, end='')
      for typ in high.keys():
         for i in high[typ].keys():
            if typ not in fsSum[fs].keys():
               continue
            if i not in fsSum[fs][typ].keys():
               continue
            if verbose:
               print(' %s %s %.0f' % (typ, i, fsSum[fs][typ][i]), end='')
      if verbose:
         print('')
   return fsSum

def checkNegative(err, t, tPrev, tLong, tmeta):
   ts = time.time()
   for (j,fs,typ,k,v) in err:
      print('job', j, 'rate -ve v', v)
      for s, n in [ (tPrev, 'tPrev'), (t, 't') ]:
         print('%s:' % n, end='')
         other = t
         if n == 't':
            other = tPrev
         for tgt in s.keys():
            if tmeta[tgt]['fs'] != fs or tmeta[tgt]['type'] != typ:
               continue
            if j not in s[tgt].keys():
               continue
            if k not in s[tgt][j].keys():
               continue
            vv =  s[tgt][j][k]
            if tgt in other.keys() and j in other[tgt].keys() and k in other[tgt][j].keys() and ( (s[tgt][j]['snapshot_time'] != other[tgt][j]['snapshot_time']) or other[tgt][j][k] != vv ):
               print(' ', tgt, k, vv, '@dt %.1f' % ( ts-s[tgt][j]['snapshot_time']), end='')
         print()

def updateLastSawTimeFromJobStats(t, lastSaw, now):
   for tgt in t.keys():
      for j in t[tgt].keys():
         if j not in lastSaw.keys():
            lastSaw[j] = 0
         lastSaw[j] = now

def slurmJobs():
   import pyslurm

   a = pyslurm.job()
   pyjobs = a.get()

   # running and suspended jobs are both active
   running = a.find('job_state', 'RUNNING')
   suspended = a.find('job_state', 'SUSPENDED')
   running.extend(suspended)

   # convert int jobids to strings to match what we get from job_stats
   running = list(map(str, running))

   return running, pyjobs

def updateLastSawTimeFromSlurm(running, lastSaw, now):
   for r in running:
      lastSaw[r] = now

def forgetOldJobs(tLong, lastSaw, job_map):
   old = time.time() - 3*jobStatsExpiry  # 3*600s ago

   # find old jobs
   oldJobs = []
   for j, ts in lastSaw.items():
      if ts < old:
         oldJobs.append(j)
   if verbose:
      print('deleting', len(oldJobs), 'jobs from tLong')

   # del from tLong
   for tgt in tLong.keys():
      toDel = []
      for j in tLong[tgt].keys():
         if j in oldJobs:
            toDel.append(j)
      for j in toDel:
         del tLong[tgt][j]

   # del from lastSaw
   for j in oldJobs:
      del lastSaw[j]

   # del from job_map
   for j in oldJobs:
      if j in job_map.keys():
         del job_map[j]

def findAllFs(tmeta):
   allFs = []
   for tgt in tmeta.keys():
      if tmeta[tgt]['fs'] not in allFs:
         allFs.append(tmeta[tgt]['fs'])
   return allFs

# parse eg. "1,3,4-8,10-20,25" or "011-058" into
#     [1,3,4,5,6,7,8,10,11, ...]  and [ 11,12,13, ... ]
def parseCpuList( cpus ):
    c = []
    s = cpus.split( ',' )
    for i in s:
        ss = i.split( '-' )
        if len(ss) > 1:  # found a range
            # check for a truncated range from slurm eg. "916-..."
            if ss[1] != '.':  # found a , "..." sigh
               c.append(int(ss[0]))
            else:
               for j in range(int(ss[0]), int(ss[1])+1):
                   c.append(j)
        else:            # found a single
            if i[0] != '.':  # found a , "..." sigh
               c.append(int(i))
    return c

def updateJobArrayMap(t, job_map, slurmjobs):
   """map jobids eg. '12366' to '12345_10'"""
   import pyslurm

   tmin = time.time() + 999  # future
   tmax = 0.0
   jobsmissed = []
   for tgt in t.keys():
      for j in t[tgt].keys():
         if j in job_map.keys():
            # already done
            continue

         # try to find array name for this job, if any
         if 0:
            print(slurmjobs.keys())
            print(list(slurmjobs.keys())[0], type(list(slurmjobs.keys())[0]))
            print('j', j, 'type(j)', type(j))
            sys.exit(1)
         if int(j) not in slurmjobs.keys():
            tm = t[tgt][j]['snapshot_time']
            tmin = min(tm, tmin)
            tmax = max(tm, tmax)
            if j not in jobsmissed:
               jobsmissed.append(j)
            #print('warning: job', j, 'not found in slurmjobs. dt', dt)
            continue

         sj = slurmjobs[int(j)]
         if sj['array_task_id'] != None and sj['array_job_id'] != None:
            # running/completed array tasks
            job_map[j] = '%d_%d' % (sj['array_job_id'], sj['array_task_id'])
         else:
            # is a non-array job
            job_map[j] = j

   if len(jobsmissed) == 0:
      return

   #print('warning: ', len(jobsmissed), 'jobs not found in slurmctld. oldest is %.2f' % (time.time()-tmin), 'newest is %.2f' % (time.time()-tmax))
   #print('jobsmissed', jobsmissed)
   #sys.exit(0)

   db = pyslurm.slurmdb_jobs()
   dbjobs = db.get(jobids=jobsmissed)
   jobsmissed2 = []
   for j in jobsmissed:
      if int(j) not in dbjobs.keys():
         jobsmissed2.append(j)
         continue
      sj = dbjobs[int(j)]
      if sj['array_task_id'] != None and sj['array_job_id'] != None:
         # running/completed array tasks
         job_map[j] = '%d_%d' % (sj['array_job_id'], sj['array_task_id'])
      else:
         # is a non-array job
         job_map[j] = j

   if len(jobsmissed2) == 0:
      return
   print('WARNING: ', len(jobsmissed2), 'jobs not found in either slurmctld or slurmdb', jobsmissed2)


def remapSrByJobsMap(sr, jobArrayMap):
   s = {}
   for j in sr.keys():
      if j not in jobArrayMap.keys():
         if verbose:
            print('skipping SrMap of (hopefully) old job', j)
         continue
      s[jobArrayMap[j]] = sr[j]
   return s


def serverCode( serverName, port ):
   # Create a TCP/IP socket
   server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
   server.setblocking(0)

   # Bind the socket to the port
   server_address = (serverName, port)
   print('starting up on %s port %s' % server_address, file=sys.stderr)
   server.bind(server_address)

   # Listen for incoming connections
   server.listen(1)

   # same again, but for port+1 which is for json requests
   jserver = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
   jserver.setblocking(0)
   jserver_address = (serverName, port+1)
   print('json server on %s port %s' % jserver_address, file=sys.stderr)
   jserver.bind(jserver_address)
   jserver.listen(1)

   # Sockets from which we expect to read
   inputs = [ server, jserver ]
   # Sockets to which we expect to write
   outputs = []

   o = {}
   loopTimeLast = time.time()  # the time we last got a block from clients
   processed = 1
   first = 1

   tLong = {}
   tPrev = {}
   jSumPrev = {}
   jSumPrevTime = 0.0
   lastSaw = {}
   jobArrayMap = {}
   jobArrayMap_copy = {}
   tmeta = {}

   # if new data, then push to influx
   newData = False

   while inputs:
      # Wait for at least one of the sockets to be ready for processing
      #print >>sys.stderr, '\nwaiting for the next event'
      timeout = 1  # seconds
      readable, writable, exceptional = select.select(inputs, outputs, inputs, timeout)
      #readable, writable, exceptional = select.select(inputs, outputs, inputs)

      if not (readable or writable or exceptional):
         #print >>sys.stderr, '  timed out, do some other work here'
         # if the interval is long then just wait 5s, otherwise wait dt/2
         loopTime = time.time()
         if not processed and loopTime - loopTimeLast > min(5.0, dt/2):
             newData = True

             # process the raw job_stats files to dicts
             parseOss(o)

             # we don't care which oss/mds the data came from, so re-arrange by 'target' (ost/mdt)
             #  - this is now t[tgt][jobid][job_stats]
             #    with metadata about t[tgt] in tmeta[tgt] ie. ['type', 'fs', 'oss']
             t = rearrangeByTgt(o, tmeta)

             # delete the raw data
             removeProcessedData(o)

             # the problem:
             # if there's no i/o in (usually) 600s, then jobs can drop out of job_stats. that doesn't mean the jobs
             # are finished. if the job does more i/o then it'll be back in job_stats, however its counters start from
             # zero again.
             #
             # the solution:
             # to deal with this, the strategy is to use t, tPrev, tLong and the timestamps on every job entry.
             # tLong stores the previous total of any job that has dropped out of t.
             # tPrev is compared with t to see what has disappeared and needs summing into tLong.
             # tSum = t + tLong is the current definitive job_stats. it is a monotically increasing and non-expiring version of job_stats.
             # tLong would grow infinitely, so jobs that have definitively gone from the system (as determined by being missing from both t and from slurm) are eventually deleted from tLong.
             #
             # we need the accurate timestamps from the raw job_stats to handle the case of a job that does say, 1MB of i/o every 600 to 630s seconds. this will regularly drop out of t, but rapidly appear again with the same i/o number. we may be coarsly sampling job_stats, so we need the timestamp to know that this 1MB is actually new, and that the old 1MB should be summed to tLong.
             #
             # tSum can be compared to the previous tSumPrev. for jobs that are in both, we can sum targets (mdts, osts) to jobs, and compute i/o rates
             # tSum is also provided as json to be consumed by other apps

             # remove all non-numeric "jobids" from the job_stats. whats left should be just slurm jobs
             nonjob = stripOutNonjobs(t)

             # loop over each entry in tPrev vs. t and see which jobs have gone
             gone = findGone(t, tPrev)

             # add gone job_stats to tLong
             addToTLong(gone, tLong)

             # if a job has an updated timestamp in t vs. tPrev, then make sure none of its fields have decreased, and at least one has gone up, otherwise it's actually a job_stats that has gone & come back, and tPrev needs to be added to tLong
             #  - note that this isn't definitive - if a job does equal or slowly increasing i/o every 60x seconds, then we can't detect that
             checkDataVsTimestamps(t, tPrev)

             # find all fs's so we can expand fs lists for some outputs
             allFs = findAllFs(tmeta)

             # t+tLong is complete.
             # sum to each fs across all targets for each job.
             # we end up with sum of ost i/o and sum of mdt i/o for each job to each fs
             # ie. jSum[job][fs][type]
             jSum = sumJob(t, tLong, tmeta)
             jSumTime = time.time()

             # compute job rates
             inv_dt = 1.0/(jSumTime - jSumPrevTime)
             r, jerr = jobRate(jSum, jSumPrev, inv_dt)

             # jobs in err got a -ve value for something. go back and check the orig data and see if it was from that or a code bug
             checkNegative(jerr, t, tPrev, tLong, tmeta)

             # print some jobs with high rates
             high = { 'mds':{'iops':100}, 'oss':{'iops':30, 'read_bytes':10**8, 'write_bytes':10**8}}
             if verbose:
                printJobRates(r, high)

             # make a short version of rates with just fields from 'high' in it.
             # also for each job, expand the filesystem list to be all fs's to make the client's job easier
             #  eg.  sr[job][fs]['oss']['iops'|'read_bytes'|'write_bytes']
             #   and sr[job][fs]['mds']['iops']
             sr = shortJobRates(r, high, allFs)
             ssr = superShortJobRates(r, high, allFs)

             # print approx whole fs rates
             #   just jobs - not including nonjob,
             #   doesn't include new jobs or jobs that have exited. ie. must be in both jSum and jSumPrev
             fsSum = fsRate(r, high)

             if verbose:
                print('tracking',len(jSum),'jobs')
                print('time', time.strftime("%Y-%m-%d %H:%M:%S"))

             # all jobs in tLong have been in t.
             # most/all jobs on the cluster should appear in t as they should all do some i/o.
             # however polls of slurm routinely miss brief jobs, and sometimes jobs are in slurm before we poll for t.

             # if a job hasn't been seen in either slurm or t for few*600s, then we can delete it from tLong.
             # so keep a record of when we last saw each job in either t or slurm
             # ie. lastSaw[job] = timestamp
             # we don't have to be accurate about this as there's no real hurry to expire jobs - tLong is just a bit larger for a while.
             now = time.time()

             # set lastSaw to 'now' for each job in t
             updateLastSawTimeFromJobStats(t, lastSaw, now)

             # loop over all running/suspended jobs in slurm and update timestamp to now too
             running, slurmjobs = slurmJobs()
             updateLastSawTimeFromSlurm(running, lastSaw, now)

             for i in running:
                if int(i) not in slurmjobs.keys():
                   print('running job', i, 'not in slurmjobs.keys()')
             #print('len(running), len(slurmjobs.keys()), len(jobArrayMap.keys())', len(running), len(slurmjobs.keys()), len(jobArrayMap.keys()))

             # jobmon/bobmon use array job syntax eg. {arrayjobid}_4, but lustre jobstats uses plain {jobid}.
             # we use the lustre jobstats id for everything in this code.
             # update the map from plain jobid to the array name so that we can export in either format...
             #
             # the main trauma here is that *really* old jobs can be in job_stats. even though their
             # snapshot_time is <900, the jobs can be gone from the system hours ago.
             # so first we query the jobs we already have from slurmctld in 'slurmjobs' and after that
             # we ask slurmdb about the rest. cache the lookups, so we only do them once.
             jobArrayMap = jobArrayMap_copy
             updateJobArrayMap(t, jobArrayMap, slurmjobs)

             # delete all super-old jobs in lastSaw from tLong
             # delete them from lastSaw and jobArrayMap too
             #
             # but first, save the current jobArrayMap so that queries before the next t poll will still return array job ids ok
             jobArrayMap_copy = dict(jobArrayMap)
             forgetOldJobs(tLong, lastSaw, jobArrayMap_copy)

             jSumDelta = getjSumDelta(jSum, jSumPrev)

             tPrev = t
             jSumPrev = jSum
             jSumPrevTime = jSumTime


             err=0
             if verbose:
                print()
             first = 0
             if err:
                print('negative rate found. resetting all rates.', file=sys.stderr)
                first = 1
             processed = 1
         continue

      # Handle inputs
      for s in readable:
         if s is server:
            # A "readable" server socket is ready to accept a connection
            connection, client_address = s.accept()
            print('new connection from', client_address, file=sys.stderr)
            connection.setblocking(0)
            inputs.append(connection)
            o[client_address] = {'size':-1}
            #print('o.keys()', o.keys())
            # any new client appearing or old client disappearing will screw up rates
            first = 1
         elif s is jserver:
            connection, client_address = s.accept()
            if verbose:
               print('new json connection from', client_address, file=sys.stderr)
            reqtype = connection.recv(1)
            if verbose:
               print('reqtype', reqtype, 'type', type(reqtype))
            if not first:
               if reqtype == b'a':  # plain jSum
                  j = json.dumps(jSum)
               elif reqtype == b'b':  # r rates
                  j = json.dumps(r)
               elif reqtype == b'c':  # short rates
                  srmap = remapSrByJobsMap(sr, jobArrayMap)
                  j = json.dumps(srmap)
               elif reqtype == b'd':  # super short rates
                  j = json.dumps(ssr)
               elif reqtype == b'e':  # fs totals
                  j = json.dumps(fsSum)
               n = connection.send(j.encode('ascii'))
               if verbose:
                  print('sent n',n, 'len(j)',len(j))
            connection.close()
         else:
            data = s.recv(102400)
            c = s.getpeername()
            if data:
               # A readable client socket has data
               #if verbose:
               #   print('received "%s" from %s, size %d' % (data, s.getpeername(), len(data)), file=sys.stderr)
               #   print('from %s, size %d' % (s.getpeername(), len(data)), file=sys.stderr)

               #print('non-server message', c)
               if o[c]['size'] == -1: # new message
                  #print('new msg')
                  if len(data) < 128:
                     print('short header. skipping', c, 'len', len(data), file=sys.stderr)
                     continue
                  try:
                     # see client section for the fields in the data header
                     hashh = str(data[96:128], encoding='ascii')
                     if hashh != hashlib.md5((str(data[:96], encoding='ascii') + secretText).encode('ascii')).hexdigest():
                        print('corrupted header. skipping. hashes do not match', data[:96], hashh, file=sys.stderr)
                        continue
                     hashb = data[64:96]
                     n = int(data[:64].strip().split()[1])
                     #print('header', data[:96], data[96:128], 'size', n)
                     o[c]['size'] = n
                     o[c]['hash'] = str(hashb, encoding='ascii')
                     o[c]['msg'] = data[128:]
                     o[c]['cnt'] = len(o[c]['msg'])
                  except:
                     print('thought it was a header, but failed', file=sys.stderr)
                     pass  # something dodgy, skip
               else:
                  # more of an in-flight message
                  #print('more. cnt', o[c]['cnt'], 'max', o[c]['size'])
                  o[c]['msg'] += data
                  o[c]['cnt'] += len(data)

               if o[c]['cnt'] == o[c]['size']: # all there?
                  #print 'all done'
                  #print >>sys.stderr, 'got all from %s, size %d. checking & unpacking' % (s.getpeername(), o[c]['size'])
                  try:
                     # done!
                     # check the hash
                     hashb = hashlib.md5(o[c]['msg']).hexdigest()
                     if hashb != o[c]['hash']:
                        print('message corrupted. hash does not match. resetting', file=sys.stderr)
                        zeroOss(o[c])
                        continue
                     # data is not corrupted. unpack
                     o[c]['data'] = pickle.loads(o[c]['msg'])

                     # shimmy the datatype up from data dict to the oss level
                     # leaving just fs data in the (non-relay) data
                     o[c]['dataType'] = o[c]['data']['dataType']
                     del o[c]['data']['dataType']

                     loopTime = time.time()
                     o[c]['time'] = loopTime
                     loopTimeLast = loopTime
                     processed = 0                        ## debug:
                     #for i in o[c]['data'].keys():
                     #   print i, len(o[c]['data'][i])
                     ## debug:
                     #j = 0
                     #for i in o.keys():
                     #   if 'data' in o[i].keys():
                     #      j += len(o[i]['data'].keys())
                     #   else:
                     #      print 'data not in oss keys of', i, 'keys', o[i].keys()
                     #print 'oss keys', len(o.keys()), 'ost keys', j
                  except:
                     print('corrupted data from', c, file=sys.stderr)

                  # put the message back into recv mode
                  # note that 'data' has not yet been processed so must be left alone
                  zeroOss(o[c])
               elif o[c]['cnt'] > o[c]['size']:
                  print('too much data. resetting', o[c]['cnt'], o[c]['size'], file=sys.stderr)
                  zeroOss(o[c])

            else:
               # Interpret empty result as closed connection
               print('closing', client_address, 'after reading no data.', file=sys.stderr)
               # Stop listening for input on the connection
               inputs.remove(s)
               s.close()
               # delete all the data from that oss too
               del o[c]
               # any new client appearing or old client disappearing will screw up rates
               first = 1

      # Handle "exceptional conditions"
      for s in exceptional:
         print('handling exceptional condition for', s.getpeername(), file=sys.stderr)
         # Stop listening for input on the connection
         inputs.remove(s)
         s.close()
         # any new client appearing or old client disappearing will screw up rates
         first = 1

      # Write data to InfluxDB
      if influxConfig is not None and newData:
         influxWrite(jSumDelta, jobArrayMap)
         newData = False



def syncToNextInterval( offset = 0 ):
   # sleep until the next interval
   t = time.time() % 60
   t += (60 + offset)   # optional time skew
   t %= 60
   i = int(t/dt)    # interval number
   sl = (i+1)*dt - t
   #print 'now', t, 'sleeping', sl , 'returning', i
   time.sleep(sl)
   return i, t

def connectSocket(sp):
   c = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
   try:
      c.connect(sp)
   except:
      print('could not connect to', sp, file=sys.stderr)
      c = None
   return c

def constructMessage(s):
   """construct header and body of message"""
   b = pickle.dumps(s)
   hashb = hashlib.md5(b).hexdigest()

   # 128 byte header
   # this needs to be a fixed size as messages get aggregated
   #
   #   length
   #  in bytes    field
   #  --------   -------
   #     7       plain text 'header '
   #     N       message length in bytes ~= 6
   #  64-N-7     padding  (room left in here)
   #    32       hash of message body
   #    32       hash of all prev bytes of this header + contents of the shared secret file

   h = 'header %d' % len(b)
   h += ' '*(64-len(h))   # room in here for more fields if we need it
   h += hashb
   hashh = hashlib.md5((h + secretText).encode('ascii')).hexdigest()
   h += hashh
   #print('h', h[:64], h[64:96], h[96:128], 'len(h)', len(h), 'len(b)==size', len(b))

   return h, b

def clientCode( serverName, port, fsList ):
   while 1:
      i, now = syncToNextInterval()
      if not dryrun:
         c = connectSocket( (serverName, port) )
         if c == None:
            time.sleep(5)
            continue

      while 1:
         t0 = time.time()
         s = {}
         s['dataType'] = 'direct'
         for f in fsList:
            s[f] = gatherStats(f)
         if verbose:
            print(s)

         h, b = constructMessage(s)
         try:
            c.send(h.encode('ascii'))
            c.send(b)
            if verbose:
               print('sent', len(h), len(b))
         except:
            if not dryrun:
               print('send of', len(h), len(b), 'failed', file=sys.stderr)
               c.close()
               break

         iNew, now = syncToNextInterval()
         if iNew != (i+1)%clientSend or now - t0 > dt:
            print('collect took too long', now-t0, 'last interval', i, 'this interval', iNew, file=sys.stderr)
         i = iNew

def usage():
   print(sys.argv[0] + '[-v|--verbose] [-d|--dryrun] [--secretfile file] [--port portnum] [--interface name] [server fsName1 [fsName2 ...]]')
   print('  server takes no args')
   print('  client needs a server name and one or more lustre filesystem names')
   print('  --verbose         - print summary of data sent to servers')
   print('  --dryrun          - do not send results to ganglia')
   print('  --secretfile file - specify an alternate shared secret file. default', secretFile)
   print('  --port portnum    - tcp port num to send/recv on. default', port)
   print('  --interface name  - make server listen on the interface that matches a hostname of "name".')
   print('                      default is to bind to the interface that matches gethostbyname')
   sys.exit(1)

def parseArgs( host ):
   global verbose, dryrun, secretFile, port, serverInterfaceName

   # parse optional args
   for v in ('-v', '--verbose'):
      if v in sys.argv:
         verbose = 1
         sys.argv.remove(v)
   for v in ('-d', '--dryrun'):
      if v in sys.argv:
         dryrun = 1
         sys.argv.remove(v)
   if '--secretfile' in sys.argv:
      v = sys.argv.index( '--secretfile' )
      assert( len(sys.argv) > v+1 )
      secretFile = sys.argv.pop(v+1)
      sys.argv.pop(v)
   if '--port' in sys.argv:
      v = sys.argv.index( '--port' )
      assert( len(sys.argv) > v+1 )
      port = int(sys.argv.pop(v+1))
      sys.argv.pop(v)
   if '--interface' in sys.argv:
      v = sys.argv.index( '--interface' )
      assert( len(sys.argv) > v+1 )
      serverInterfaceName = sys.argv.pop(v+1)
      sys.argv.pop(v)

   if len(sys.argv) == 1:
      return host, None # server takes no args
   if len(sys.argv) < 3:
      usage()
   return sys.argv[1], sys.argv[2:]


def readInfluxConfig():
   global influxConfig

   if os.path.isfile(influxConfigFile):
      with open(influxConfigFile) as f:
         l = [x.strip() for x in f.readlines()]
      influxConfig = {
         'server': l[0],
         'org': l[1],
         'bucket': l[2],
         'token': l[3],
      }
      print(f'influxDB config file read: {influxConfigFile}')

   else:
      print(f'{influxConfigFile} does not exist, disabling influx writing')
      # if file doesn't exist, turn off writing to influx
      influxConfig = None

def getjSumDelta(jSum, jSumPrev):
   # jSum contains all historical data points - calculate the delta between the previous and current data
   jSumDelta = {}
   for jobid in jSum.keys():
      if jobid not in jSumPrev.keys():
         # If the jobid was not in the previous data, then all points are new
         jSumDelta[jobid] = jSum[jobid]
         continue
      else:
         jSumDelta[jobid] = {}

         for fs in jSum[jobid].keys():
            # If the fs was not in the previous data, then all points are new
            if fs not in jSumPrev[jobid].keys():
               jSumDelta[jobid][fs] = jSum[jobid][fs]
               continue
            else:
               jSumDelta[jobid][fs] = {}

            for server in jSum[jobid][fs].keys():
               # If the server was not in the previous data, then that point is new
               if server not in jSumPrev[jobid][fs].keys():
                  jSumDelta[jobid][fs][server] = jSum[jobid][fs][server]
                  continue

               # Otherwise, check the timestamp to see if that point is new
               elif jSum[jobid][fs][server]["snapshot_time"] > jSumPrev[jobid][fs][server]["snapshot_time"]:
                     jSumDelta[jobid][fs][server] = jSum[jobid][fs][server]

   return jSumDelta

def influxWrite(jSumDelta, jobArrayMap):
   from influxdb_client import InfluxDBClient

   if verbose:
      print(f"writing data to {influxConfig['server']}")

   client = InfluxDBClient(
      url=influxConfig['server'],
      token=influxConfig['token'],
      org=influxConfig['org'],
   )

   influxConfig['server']

   write_api = client.write_api()

   data = []

   # Send the new points to InfluxDB
   for jobid in jSumDelta:

      # If jobid is not in jobArrayMap, then this is not a real slurm job
      if jobid in jobArrayMap:
         for fs in jSumDelta[jobid]:
            for server in jSumDelta[jobid][fs]:
               lustre_fields = jSumDelta[jobid][fs][server]

               # Get current timestamp
               now = time.time()
               diff = int(now - lustre_fields["snapshot_time"])

               # If the data is more than 1 hour old, print a warning and don't send to influx
               cutoff_time = 3600
               if diff > cutoff_time:
                  print(f'warning: jobstats for {jobid} are {diff} seconds old, not sending to influx')

               else:
                  # Only keep fields used by jobmon
                  jobmon_fields = ["read_bytes", "write_bytes", "iops"]
                  fields = {k: lustre_fields[k] for k in lustre_fields if k in jobmon_fields}

                  data += [
                     {
                        'measurement': 'lustre',
                        'tags': {'job': jobArrayMap[jobid], 'fs': fs, 'server': server},
                        'fields': fields,
                        'time': lustre_fields["snapshot_time"],
                     }
                  ]

   if verbose:
      print(f"writing {len(data)} data points to influx")

   # Dictionary-style write
   write_api.write(
      influxConfig['bucket'],
      influxConfig['org'],
      data,
      write_precision='s'
   )

   # Flush writes
   write_api.close()

if __name__ == '__main__':
   host = socket.gethostname()
   serverName, fsList = parseArgs( host )
   readSecret()
   readInfluxConfig()
   if host == serverName:
      if serverInterfaceName != None:
          serverName = serverInterfaceName
      serverCode( serverName, port ) # server recv code
   else:
      if serverInterfaceName != None:
         print('error: --interface is an option for the server only')
         usage()
      clientCode( serverName, port, fsList ) # client send code
