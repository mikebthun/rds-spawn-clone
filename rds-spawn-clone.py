#!/usr/bin/python -tt

import sys,getopt
import commands 
import logging
import json
import datetime
import dateutil.parser
import operator
import time

logger = logging.getLogger('stencil')
hdlr = logging.StreamHandler(sys.stdout)
#hdlr = logging.FileHandler('stencil.log') 
formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
hdlr.setFormatter(formatter)
logger.addHandler(hdlr) 
logger.setLevel(logging.ERROR) #logging.DEBUG


def help():

  print " Usage: %s [--help] " % sys.argv[0]
  sys.exit(2)



def Run(cmd):
 
  logger.info("* Running: %s" % cmd)

  (status,output) = commands.getstatusoutput(cmd)

  if status > 0:
    logger.error(cmd)
    logger.error(output)
    sys.exit(2)

  logger.info("[OK]")
  return output

def restoreComplete(output):
 
  # parse json
  try:
    j = json.loads(output)
    status=j['DBInstances'][0]['DBInstanceStatus']
  except Exception, e:
    print e
    logger.error("Could not parse get last snapshot json")
    sys.exit(2)

  if status == "available":
    return True

  logger.info("Waiting on database restore: %s", status)
  return None

def main(argv):
 
  instanceType="db.m3.medium"
  target=None
  blocking=True

  # make sure command line arguments are valid
  try:
    options, args = getopt.getopt(

       argv, 
      'hv', 
      [ 
        'help',
        'verbose',
        'target='
    
      ])
 
  except getopt.GetoptError:
    logging.fatal("Bad options!")
    help()
    sys.exit(2)


  # handle command line arugments
  for opt, arg in options:
    if opt in ('-h', '--help'):
      help()
      sys.exit(2)
    elif opt in ('', '--target'):
      target=arg
    elif opt in ('-v', '--verbose'):
      logger.setLevel(logging.DEBUG)

  if target == None:
    logger.error("Missing target")
    help()
 


  ###################################
  # main code starts here
  ###################################

  # get latest snapshot
  cmd = "aws rds describe-db-snapshots --db-instance-identifier %s --snapshot-type automated" % target
  output = Run(cmd)
  try:
    j = json.loads(output)
    snapshotsRaw=j['DBSnapshots']
  except Exception, e:
    print e
    logger.error("Could not parse get last snapshot json")
    sys.exit(2)

  if not len(snapshotsRaw):
    logger.error("No snapshots found for this instance")
    sys.exit(2)

  snapshots = {}

  for snapshotRaw in snapshotsRaw:
    if snapshotRaw['Status'] == 'available':
      snapshots[snapshotRaw['DBSnapshotIdentifier']]=dateutil.parser.parse(snapshotRaw['SnapshotCreateTime'])
 
  snapshots = sorted(snapshots.items(),key=operator.itemgetter(1),reverse=True)

  snapshot = snapshots[0][0] # first one (newest)
  createTime = snapshots[0][1]
  
  # check how old it is
  now = datetime.datetime.now()
  delta = now-createTime.replace(tzinfo=None)
  if delta.days > 2:
    logger.error("%d days have elapsed since last snapshot" % delta.days )
    sys.exit(2)
 

  restoreName="%s-%d"[:63] % (snapshot.replace("rds:", "restore-"), time.time())
  cmd = "aws rds restore-db-instance-from-db-snapshot --publicly-accessible --db-instance-identifier %s --tags Key=rds-dr-walkaway,Value=%s --db-snapshot-identifier %s --db-instance-class %s" % (
    restoreName,
    snapshot,
    snapshot,
    instanceType

  )

  output=Run(cmd)
  

  if blocking:

    # wait for snapshot to complete
    complete=None
    timeout=40 # 40 minutes

    for i in range(timeout):
      logger.info("Checking restore status, elapased: %d minutes", i)
      time.sleep(60)
      cmd = "aws rds describe-db-instances --db-instance-identifier %s" % restoreName
      output = Run(cmd)

      if restoreComplete(output):
        break

    if not restoreComplete(output):
      logger.error("BAD - Database never restored")
      sys.exit(2)

    print output


  else: # non-blocking just return
    print output


if __name__ == "__main__":
  main(sys.argv[1:])
 
 
