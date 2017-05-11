#!/usr/bin/env python

import os
import sys
import urllib
import httplib
import re
import json
from pprint import pprint
from copy import deepcopy
from urllib import urlencode
from urlparse import urljoin


#url = "cmsweb.cern.ch"
#url = "cmsweb-testbed.cern.ch"
url = "alan-cloud1.cern.ch"


def getStepChainArgs():
    "DO_NOT_USE: Hack method"
    schema = {"Memory": 2222,
              "ScramArch": "slc6_amd64_gcc481",
              "Step1": {"BlockWhitelist": [], "Campaign": "TEST-Alan-Override"},
              "Step3": {"LumisPerJob": 5}}
    return schema

def getTaskChainArgs():
    "DO_NOT_USE: Hack method"
    schema = {"Memory": 2444,
              "TimePerEvent": 2.5,
              "Task1": {"TransientOutputModules": [], "LumiList": {"202205": [[1,20], [101, 110]]}},
              "Task2": {"LumisPerJob": 2}}
    return schema

def createClone(originalRequest, schema={}):
    headers = {"Content-type": "application/json",
               "Accept": "application/json"}
    #schema = getStepChainArgs()
    #schema = getTaskChainArgs()

    encodedParams = json.dumps(schema)
    conn  = httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
    print "Cloning request '%s' with the following override settings: %s" % (originalRequest, schema)
    conn.request("POST", "/reqmgr2/data/request/clone/%s" % originalRequest, encodedParams, headers)
    resp = conn.getresponse()
    data = resp.read()
    if resp.status != 200:
        print "Response status: %s\tResponse reason: %s" % (resp.status, resp.reason)
        print "Error message: %s" % resp.msg.getheader('X-Error-Detail')
        sys.exit(1)

    #print data
    data = json.loads(data)
    requestName = data['result'][0]['request']
    print "Clone: %s" % requestName
    return requestName



if __name__ == "__main__":
    if len(sys.argv) != 2:
        print "Usage:"
        print "python clone.y WORKFLOW_NAME"
        sys.exit(0)

    workflow = sys.argv[1]
    newWorkflow=createClone(workflow)

    sys.exit(0)
