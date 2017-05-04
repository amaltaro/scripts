"""
Pre-requisites:
 1. a valid proxy in your X509_USER_PROXY variable
 2. wmagent env: source /data/srv/wmagent/current/apps/wmagent/etc/profile.d/init.sh
"""

import sys
import os
import json
import httplib
from copy import copy
from pprint import pprint

url = "cmsweb.cern.ch"
reqmgrCouchURL = "https://" + url + "/couchdb/reqmgr_workload_cache"

DEFAULT_DICT = {
    "AcquisitionEra": "UPDATEME",
    "CMSSWVersion": "UPDATEME",
    "Campaign": "UPDATEME",
    "ConfigCacheUrl": "https://cmsweb.cern.ch/couchdb",
    "DQMConfigCacheID": "UPDATEME",
    "DQMHarvestUnit": "UPDATEME",
    "DQMUploadUrl": "UPDATEME",
    "DbsUrl": "https://cmsweb.cern.ch/dbs/prod/global/DBSReader/",
    "GlobalTag": "UPDATEME",
    "InputDataset": "UPDATEME",
    "Memory": 2200,
    "PrepID": "UPDATEME",
    "ProcessingString": "UPDATEME",
    "ProcessingVersion": 1,
    "RequestPriority": 999999,
    "RequestString": "UPDATEME",
    "RequestType": "DQMHarvest",
    "ScramArch": "UPDATEME",
    "SizePerEvent": 1600,
    "TimePerEvent": 1}


def main():
    if len(sys.argv) != 2:
        print "Usage: python injectHarvest.py WORKFLOW_NAME"
        sys.exit(0)

    work = retrieveWorkload(sys.argv[1])
    newDict = buildRequest(work)
    pprint(newDict)
    workflow = submitWorkflow(newDict)
    approveRequest(workflow)
    sys.exit(0)


def retrieveWorkload(workflowName):
    headers = {"Content-type": "application/json",
               "Accept": "application/json"}
    conn = httplib.HTTPSConnection(url, cert_file=os.getenv('X509_USER_PROXY'), key_file=os.getenv('X509_USER_PROXY'))
    urn = "/reqmgr2/data/request/%s" % workflowName
    conn.request("GET", urn, headers=headers)
    r2 = conn.getresponse()
    request = json.loads(r2.read())["result"][0][workflowName]
    return request


def buildRequest(req_cache):
    newSchema = copy(DEFAULT_DICT)
    for k, v in DEFAULT_DICT.iteritems():
        if v != "UPDATEME":
            pass
        else:
            if k == 'RequestString':
                newSchema[k] = req_cache[k] + '_Harv'
            elif k == 'InputDataset':
                dset = [d for d in req_cache['OutputDatasets'] if d.endswith(tuple(['/DQM', '/DQMIO']))]
                newSchema[k] = dset[0]
            else:
                if isinstance(req_cache[k], dict):
                    # then simply pick the first value, makes no difference in the end
                    newSchema[k] = req_cache[k].values()[0]
                else:
                    newSchema[k] = req_cache[k]

    return newSchema


def submitWorkflow(schema):
    headers = {"Content-type": "application/json",
               "Accept": "application/json"}
    encodedParams = json.dumps(schema)
    conn = httplib.HTTPSConnection(url, cert_file=os.getenv('X509_USER_PROXY'), key_file=os.getenv('X509_USER_PROXY'))
    print "Submitting new workflow..."
    conn.request("POST", "/reqmgr2/data/request", encodedParams, headers)
    resp = conn.getresponse()
    data = resp.read()
    if resp.status != 200:
        print "Response status: %s\tResponse reason: %s" % (resp.status, resp.reason)
        if hasattr(resp.msg, "x-error-detail"):
            print "Error message: %s" % resp.msg["x-error-detail"]
            sys.exit(1)
    data = json.loads(data)
    requestName = data['result'][0]['request']
    print "  Request '%s' successfully created." % requestName
    return requestName


def approveRequest(workflow):
    print "Approving request..."
    encodedParams = json.dumps({"RequestStatus": "assignment-approved"})
    headers = {"Content-type": "application/json",
               "Accept": "application/json"}

    conn = httplib.HTTPSConnection(url, cert_file=os.getenv('X509_USER_PROXY'), key_file=os.getenv('X509_USER_PROXY'))
    conn.request("PUT", "/reqmgr2/data/request/%s" % workflow, encodedParams, headers)
    resp = conn.getresponse()
    data = resp.read()
    if resp.status != 200:
        print "Response status: %s\tResponse reason: %s" % (resp.status, resp.reason)
        if hasattr(resp.msg, "x-error-detail"):
            print "Error message: %s" % resp.msg["x-error-detail"]
            sys.exit(2)
    conn.close()
    print "  Request successfully approved!"
    return


if __name__ == '__main__':
    main()
