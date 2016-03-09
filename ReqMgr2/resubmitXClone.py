#!/usr/bin/env python

import os
import sys
import urllib
import httplib
import re
import json
from pprint import pprint
from copy import deepcopy
from WMCore.WMSpec.WMWorkload import WMWorkloadHelper

#url = "cmsweb.cern.ch"
url = "alan-cloud1.cern.ch"
#url = "cmsweb-testbed.cern.ch"
#url2 = "cmsweb-testbed.cern.ch"

reqmgrCouchURL = "https://"+url+"/couchdb/reqmgr_workload_cache"

def approveRequest(url, workflow):
    print "Approving request..."
    encodedParams = json.dumps({"RequestStatus": "assignment-approved"})
    headers = {"Content-type": "application/json",
               "Accept": "application/json"}

    conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
    conn.request("PUT",  "/reqmgr2/data/request/%s" % workflow, encodedParams, headers)
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


def changePrio(url, workflow, prio):
    print "Changing priority for %s ..." % workflow
    encodedParams = json.dumps({"RequestPriority": prio})
    headers = {"Content-type": "application/json",
               "Accept": "application/json"}

    conn = httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
    conn.request("PUT",  "/reqmgr2/data/request/%s" % workflow, encodedParams, headers)
    resp = conn.getresponse()
    data = resp.read()
    if resp.status != 200:
        print "Response status: %s\tResponse reason: %s" % (resp.status, resp.reason)
        if hasattr(resp.msg, "x-error-detail"):
            print "Error message: %s" % resp.msg["x-error-detail"]
        sys.exit(2)
    conn.close()
    print "  Request priority successfully changed to %s!" % prio
    return


def retrieveSchema(workflowName):
    specURL = os.path.join(reqmgrCouchURL, workflowName, "spec")
    helper = WMWorkloadHelper()
    helper.load(specURL)

    paramBlacklist = ['BlockCloseMaxEvents', 'BlockCloseMaxFiles', 'BlockCloseMaxSize', 'BlockCloseMaxWaitTime',
                      'CouchURL', 'CouchWorkloadDBName', 'GracePeriod', 'Group', 'HardTimeout', 'InitialPriority',
                      'inputMode', 'OutputDatasets', 'ReqMgr2Only', 'Requestor', 'RequestDate' 'RequestorDN',
                      'RequestName', 'RequestStatus', 'RequestTransition', 'RequestWorkflow', 'SiteWhitelist',
                      'SoftTimeout', 'SoftwareVersions', 'Team', 'timeStamp']
    schema = {}
    for (key, value) in helper.data.request.schema.dictionary_whole_tree_().iteritems():
        if not value or key in paramBlacklist:
            continue
        else:
            schema[key] = value

    schema['Requestor'] = 'amaltaro'
    schema['Group'] = 'DATAOPS'

    return schema


def submitWorkflow(schema):
    # clean up schema from reqmgr2
    for schemaListItem in ["RunWhitelist", "RunBlacklist", "BlockWhitelist", "BlockBlacklist"]:
        if schemaListItem in schema.keys():
            schema[schemaListItem] = str(schema[schemaListItem])
    headers = {"Content-type": "application/json",
               "Accept": "application/json"}
    encodedParams = json.dumps(schema)
    conn  = httplib.HTTPSConnection(url2, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
    print "Submitting new workflow..."
    conn.request("POST",  "/reqmgr2/data/request", encodedParams, headers)
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


if __name__ == "__main__":
    if len(sys.argv) != 2 and len(sys.argv) != 3:
        print "Usage:"
        print "python resubmitXClone.y WORKFLOW_NAME PRIORITY"
        sys.exit(0)

    workflow = sys.argv[1]
    if len(sys.argv) == 3:
        prio = int(sys.argv[2])
        print type(prio)
        changePrio(url, workflow, prio)
    else:
        schema = retrieveSchema(workflow)
        print "Retrieved schema (after a bit of cleanup):"
        pprint(schema)
    #    sys.exit(0)
        newWorkflow=submitWorkflow(schema)
        approveRequest(url2, newWorkflow)

    sys.exit(0)
