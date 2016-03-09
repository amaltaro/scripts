#!/usr/bin/env python
import urllib2,urllib, httplib, sys, re, os
import json
import optparse
from pprint import pprint


def assignRequest(url, workflow, team, site, era, procstr, procver, activity, lfn, maxrss, trust):
    params = {"RequestStatus": "assigned",
              "Team": team,
              "SiteWhitelist": site,
              "SiteBlacklist": [],
              "AcquisitionEra": era,
              "ProcessingString": procstr,
              "ProcessingVersion": procver,
              "Dashboard": activity,
              "MergedLFNBase": lfn,
              "MaxRSS": maxrss,
              "TrustSitelists" : trust,   ### when we want to use xrootd to readin input files
              "UnmergedLFNBase": "/store/unmerged",
              "MinMergeSize": 2147483648,
              "MaxMergeSize": 4294967296,
              "MaxMergeEvents": 100000,
              "MaxVSize": 20294967,
#              "CustodialSites": ['T1_US_FNAL'],
#              "CustodialSubType" : "Replica",
#              "NonCustodialSites": ['T2_CH_CERN'],
#              "NonCustodialSubType" : "Replica",
#              "AutoApproveSubscriptionSites": ['T2_CH_CERN'],
#              "SubscriptionPriority": "Normal",
#              "BlockCloseMaxWaitTime" : 15000,
              "BlockCloseMaxWaitTime" : 86400,
              "BlockCloseMaxFiles" : 500,
              "BlockCloseMaxEvents" : 20000000,
              "BlockCloseMaxSize" : 5000000000000,
              "SoftTimeout" : 129600,
              "GracePeriod" : 1000}
    # Once the AcqEra is a dict, I have to make it a json object 
    json_args = json.dumps(params)

    headers = {"Content-type": "application/json",
               "Accept": "application/json"}

    conn = httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
    urn = "/reqmgr2/data/request/%s" % workflow
    conn.request("PUT", urn, json_args, headers=headers)

    resp = conn.getresponse()
    data = resp.read()
    if resp.status != 200:
        print "Response status: %s\tResponse reason: %s" % (resp.status, resp.reason)
        if hasattr(resp.msg, "x-error-detail"):
            print "Error message: %s" % resp.msg["x-error-detail"]
            sys.exit(2)
    conn.close()
    print 'Assigned workflow:',workflow,'to site:',site,'and team',team


def getRequestDict(url, workflow):
    headers = {"Content-type": "application/json",
               "Accept": "application/json"}
    conn = httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
    urn = "/reqmgr2/data/request/%s" % workflow
    r1=conn.request("GET", urn, headers=headers)
    r2=conn.getresponse()
    request = json.loads(r2.read())["result"][0]
    return request[workflow]


def main():
#    url='alan-cloud1.cern.ch'	
    url='cmsweb-testbed.cern.ch'	
#    url='cmsweb.cern.ch'	
    ### Example: python assignWFtestbed.py -w amaltaro_TaskChain_Data_ReqMgr2_Validation_v1_160303_135754_1696 -t alan-devvm --test
    parser = optparse.OptionParser()
    parser.add_option('-w', '--workflow', help='Workflow Name',dest='workflow')
    parser.add_option('-t', '--team', help='Type of Requests',dest='team')
    parser.add_option('-s', '--site', help='Site',dest='site')
    parser.add_option('-p', '--procversion', help='Processing Version',dest='procversion')
    parser.add_option('-a', '--activity', help='Dashboard Activity',dest='activity')
    parser.add_option('-l', '--lfn', help='Merged LFN base',dest='lfn')

    parser.add_option('--addStr', help='It will append this string to the AcqEra', default='', dest='addStr')
    parser.add_option('--test', action="store_true", help='Nothing is injected, only print infomation about workflow and AcqEra',dest='test')
    parser.add_option('--trust', action="store_true", help='Flag to trust site white list', default=False, dest='trust')
    (options,args) = parser.parse_args()

    if not options.workflow:
        print "The workflow name is mandatory!"
        print "Usage: python assignWFtestbed.py -w <requestName>"
        sys.exit(0);
    workflow=options.workflow
    team='testbed-vocms009'
    site=['T1_US_FNAL','T2_CH_CERN']
    procversion=9
    activity='test'
    #lfn='/store/relval'
    lfn='/store/backfill/1'
    acqera = {}
    procstring = {}
    maxRSS = {}
    addStr = options.addStr
    trust = options.trust

    ### Getting the original dictionary
    schema = getRequestDict(url,workflow)

    if not schema['RequestStatus'] in ['assignment-approved', 'failed']:
        print "Cannot assign workflow %s in status %s" % (workflow, schema['RequestStatus'])
        sys.exit(0)

    if schema['RequestType'] == 'TaskChain':
        tasks = ['Task%s' % i for i in range(1, schema['TaskChain']+1)]
        for task in tasks:
            taskDict = schema[task]
            if 'LheInputFiles' in taskDict and taskDict['LheInputFiles']:# in [True, 'True']:
                site="T2_CH_CERN"
            procstring[taskDict['TaskName']] = taskDict.get(u'ProcessingString', 'ReqMgr2_Validation_TEST').replace("-","_")
            procstring[taskDict['TaskName']] += addStr
            if 'AcquisitionEra' in schema or 'AcquisitionEra' in taskDict:
                acqera[taskDict['TaskName']] = taskDict.get('AcquisitionEra', 'DMWM_Test').replace("-","_")
            else:
                acqera[taskDict['TaskName']] = taskDict.get('Campaign', schema['Campaign']).replace("-","_")
            maxRSS[taskDict['TaskName']] = (taskDict.get('Memory', schema['Memory']) + 1000) * 1024
    else:
        acqera = schema.get('AcquisitionEra', schema['Campaign'])
        procstring = schema.get('ProcessingString', 'ReqMgr2_Validation_TEST_ALAN') + addStr
        maxRSS = schema.get('Memory', 2400) * 1024

    if 'LheInputFiles' in schema and schema['LheInputFiles']:# in [True, 'True']:
        site="T2_CH_CERN"
    # Handling the parameters given in the command line
    if options.team:
        team=options.team
    if options.site:
        site=options.site
    if options.procversion:
        procversion=int(options.procversion)
    if options.activity:
        activity=options.activity
    if options.lfn:
        lfn=options.lfn

    # If the --test argument was provided, then just print the information gathered so far and abort the assignment
    if options.test:
        print workflow, '\tAcqEra:', acqera, '\tProcStr:', procstring, '\tProcVer:', procversion, '\tMaxRSS:', maxRSS, '\tTeam:',team, '\tSite:', site, '\tTrust:', trust
        sys.exit(0)

    # Really assigning the workflow now
    print workflow, '\tAcqEra:', acqera, '\tProcStr:', procstring, '\tProcVer:', procversion, '\tMaxRSS:', maxRSS, '\tTeam:',team, '\tSite:', site, '\tTrust:', trust
    assignRequest(url, workflow, team, site, acqera, procstring, procversion, activity, lfn, maxRSS, trust)
    sys.exit(0);

if __name__ == "__main__":
    main()
