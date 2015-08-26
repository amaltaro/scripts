"""
Pre-requisites:
 1. a valid proxy in your X509_USER_PROXY variable
 2. wmagent env: source /data/srv/wmagent/current/apps/wmagent/etc/profile.d/init.sh
"""

import sys
import os
import re
import shlex
import json
import urllib
import httplib
from optparse import OptionParser
from subprocess import call
from time import sleep
from pprint import pprint
from WMCore.WMSpec.WMWorkload import WMWorkloadHelper

url = "cmsweb.cern.ch"
reqmgrCouchURL = "https://"+url+"/couchdb/reqmgr_workload_cache"

DEFAULT_DICT = {#'CouchURL': 'https://cmsweb.cern.ch/couchdb',
                'ConfigCacheUrl': "https://cmsweb.cern.ch/couchdb",
                'DbsUrl': 'https://cmsweb.cern.ch/dbs/prod/global/DBSReader',
                'Group': 'DATAOPS',
                'Memory': 2200,
                'OpenRunningTimeout': 0,
                'RequestPriority': 140000,
                'RequestType': 'DQMHarvest',
                'Requestor': 'amaltaro',
                'mergedLFNBase': '/store/data',
                'unmergedLFNBase': '/store/unmerged'}

UPDATE_KEYS = ['InputDataset', 'Scenario', 'ProcessingVersion',
               'ProcessingString', 'ScramArch', 'SizePerEvent',
               'DQMConfigCacheID', 'RequestString', 'DQMUploadUrl',
               'GlobalTag', 'Campaign', 'TimePerEvent', 'AcquisitionEra',
               'CMSSWVersion']


def main():
    if len(sys.argv) != 2:
        print "Usage: python injectHarvest.py WORKFLOW_NAME"
        sys.exit(0)

    work = retrieveWorkload(sys.argv[1])
    newDict = buildRequest(work)
    pprint(newDict)
    workflow = submitWorkflow(newDict)
    print workflow
    approveRequest(workflow)
    sys.exit(0)


def retrieveWorkload(workflowName):
    conn = httplib.HTTPSConnection(url, 
                                   cert_file = os.getenv('X509_USER_PROXY'),
                                   key_file = os.getenv('X509_USER_PROXY'))
    r1 = conn.request('GET', '/couchdb/reqmgr_workload_cache/' + workflowName)
    r2 = conn.getresponse()
    data = r2.read()
    conn.close()
    s = json.loads(data)
    #pprint(s)
    return s


def buildRequest(req_cache):
    newSchema = DEFAULT_DICT
    for k in UPDATE_KEYS:
        if k == 'RequestString':
            newSchema[k] = req_cache[k] + '_Harv'
        elif k == 'InputDataset':
            dset = [d for d in req_cache['OutputDatasets'] if d.endswith(tuple(['/DQM', '/DQMIO']))]
            newSchema[k] = dset[0]
        elif k == 'ProcessingVersion':
            newSchema[k] = int(req_cache[k])
        else:
            newSchema[k] = req_cache[k]
        if isinstance(newSchema[k], basestring):
          newSchema[k] = str(newSchema[k])
        else:
          newSchema[k] = int(newSchema[k])
    #pprint(newSchema)
    return newSchema


def submitWorkflow(schema):
    for schemaListItem in ["RunWhitelist", "RunBlacklist", "BlockWhitelist", "BlockBlacklist"]:
        if schemaListItem in schema.keys():
            schema[schemaListItem] = str(schema[schemaListItem])

    jsonEncodedParams = {}
    for paramKey in schema.keys():
        jsonEncodedParams[paramKey] = json.dumps(schema[paramKey])

    encodedParams = urllib.urlencode(jsonEncodedParams, False)

    headers = {"Content-type": "application/x-www-form-urlencoded",
                 "Accept": "text/plain"}

    conn = httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
    print "\nsubmitting new workflow to %s" % url
    conn.request("POST",  "/reqmgr/create/makeSchema", encodedParams, headers)
    response = conn.getresponse()
    #print response.status, response.reason
    data = response.read()
    #print data
    details=re.search("details\/(.*)\'",data)
    return details.group(1)


def approveRequest(workflow):
    params = {"requestName": workflow, "status": "assignment-approved"}

    encodedParams = urllib.urlencode(params)
    headers  =  {"Content-type": "application/x-www-form-urlencoded",
                 "Accept": "text/plain"}

    conn  =  httplib.HTTPSConnection(url,
                                     cert_file = os.getenv('X509_USER_PROXY'),
                                     key_file = os.getenv('X509_USER_PROXY'))
    conn.request("PUT",  "/reqmgr/reqMgr/request", encodedParams, headers)
    response = conn.getresponse()
    if response.status != 200:
        print 'could not approve request with following parameters:'
        for item in params.keys():
            print item + ": " + str(params[item])
        print 'Response from http call:'
        print 'Status:',response.status,'Reason:',response.reason
        print 'Explanation:'
        data = response.read()
        print data
        print "Exiting!"
        sys.exit(1)
    conn.close()
    return


if __name__ == '__main__':
    main()

