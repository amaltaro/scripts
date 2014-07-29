#!/usr/bin/env python
"""
WorkloadMon.py: fetch the workloadsummary from couch and make performance plots.

This script is meant to parse all the performance fields present in a workload
summary (stored in couchdb) and then nicely plot the metrics you are interested
in.

General development plan:
 1) code its UI: input via CLI and output in text mode only
 2) code its GUI: input via CLI and output in graphics (accessible via web)
 3) code its server side: input and output via webserver

Request types:
- MonteCarlo        done    jbalcas_MonteCarloEFFb1_140521_151902_3845
- MonteCarlo        done    amaltaro_TOP-Summer11LegwmLHE-00006_140728_142125_9306
- MonteCarlo LHE    done    amaltaro_SUS-Summer12pLHE-00144_140728_142123_189
- MonteCarloFromGEN done    amaltaro_B2G-Summer12-00736_140728_142131_8982
- ReDigi 1 step     done    amaltaro_BPH-Spring14miniaod-00004_140728_151007_4877
- ReDigi 2 steps    done    amaltaro_B2G-Summer12DR53X-00743_140729_100547_7821
- ReReco            done    amaltaro_ObjID2012DDoubleElectron_140729_101411_3131
- TaskChain         ?
"""
import sys, os
import httplib, json
import pprint
from optparse import OptionParser

#import sys,urllib,urllib2,re,time,os
#import datetime
#import itertools
#import matplotlib
#matplotlib.use('agg')
#import matplotlib.pyplot as pp
#import numpy as np

cmsweb_url = 'cmsweb-testbed.cern.ch'

def getWorkloadSummary(request):
    """
    Receives the requestName as input and then fetch its workloadsummary
    in couchdb. Returns a json object.
    """
    conn = httplib.HTTPSConnection(cmsweb_url, \
           cert_file = os.getenv('X509_USER_PROXY'), \
           key_file = os.getenv('X509_USER_PROXY'))
    r1 = conn.request('GET', '/couchdb/workloadsummary/' + request)
    r2 = conn.getresponse()
    data = r2.read()
    s = json.loads(data)
    conn.close()
    # TODO: improve error report
    #except:
    #   print "Cannot get request (getWorkloadSummary) " 
    #   sys.exit(1)
    return s

def main():
    """
    First you need to source cms_ui env, so you get your X509_USER_PROXY exported.
    Then you provide a requestName that will be fetched from couchdb in order to
    be mined and have its performance information nicely shown.
    """
    usage = "Usage: %prog -r requestName"
    parser = OptionParser(usage = usage)
    parser.add_option('-r', '--request', help = 'Request name', dest = 'request')
    (options, args) = parser.parse_args()
    if not options.request:
        parser.error('You must provide a request name')
        sys.exit(1)

    request = options.request
    reqout = getWorkloadSummary(request)
    #pprint.pprint(reqout)

    # TODO: also print the requestType and the siteWhiteList (needs reqmgr call)
    print 'Request name   : %s' % reqout['_id']
    print 'Campaign       : %s' % reqout['campaign']
    print 'Input dataset  : %s' % reqout['inputdatasets']
    print 'Output dataset :'
    for out in reqout['output'].keys():
        print ' - %s' % out

    # Dict of whitelisted task:path values
    tasks = {}
    taskBlacklist = ['Merge', 'LogCollect', 'Cleanup', 'Harvesting']
    for pathTask in reqout['performance'].keys():
        taskName = pathTask.split('/')[-1]
        for i in taskBlacklist:
            if i in taskName:
                break
        else:
            tasks[taskName] = pathTask

    # Time to work on the performance thing
    allMetrics = {}
    allMetrics['timing'] = ['jobTime', 'TotalJobTime', 'AvgEventTime', 'MaxEventTime', \
                            'MinEventTime', 'writeTotalSecs']
    allMetrics['memory'] = ['PeakValueVsize', 'PeakValueRss']
    allMetrics['cpu']    = ['TotalEventCPU', 'AvgEventCPU', 'MaxEventCPU', 'MinEventCPU', \
                            'TotalJobCPU']
    allMetrics['disk']   = ['writeTotalMB', 'readTotalMB', 'readAveragekB', 'readMBSec', \
                            'readNumOps', 'readPercentageOps', 'readCachePercentageOps', \
                            'readTotalSecs', 'readMaxMSec']

    #goldenMetrics = ['PeakValueVsize', 'AvgEventTime', 'TotalJobTime', 'PeakValueRss']

    print '\nPerformance    :'
    # pair of task:path values
    for t, p in tasks.iteritems():
        myMetrics = {}
        print ' - Path  : %s' % p
        print ' - Task  : %s' % t
        for m, value in reqout['performance'][p]['cmsRun1'].iteritems():
            try:
                myMetrics[m] = value['average']
            except KeyError:
                # then have to iterate over the whole histogram
                avg = 0
                nEvents = 0
                for element in value['histogram']:
                    nEvents += element['nEvents']
                    avg += element['average'] * element['nEvents']
                myMetrics[m] = avg / nEvents
            print '    %-23s: %s' % (m, myMetrics[m])

    # TODO: If I want to get the worstOffenders, then I should always pick up the first
    # element in the list

    # TODO: analyse the results and build up a documentation for these metrics
    print '\nWork done!'
    sys.exit(0)

if __name__ == "__main__":
        main()
