#!/usr/bin/env python3
from __future__ import print_function

import json
import os
import sys
import http.client

#url = "cmsweb.cern.ch"
url = "cmsweb-testbed.cern.ch"
#url = "alancc7-cloud2.cern.ch"


def createClone(originalRequest, schema=None):
    schema = schema or {}
    headers = {"Content-type": "application/json",
               "Accept": "application/json"}

    encodedParams = json.dumps(schema)
    conn = http.client.HTTPSConnection(url, cert_file=os.getenv('X509_USER_PROXY'),
                                       key_file=os.getenv('X509_USER_PROXY'))
    print("Cloning request '%s' with the following override settings: %s" % (originalRequest, schema))
    conn.request("POST", "/reqmgr2/data/request/clone/%s" % originalRequest, encodedParams, headers)
    resp = conn.getresponse()
    data = resp.read()
    if resp.status != 200:
        print("Response status: %s\tResponse reason: %s" % (resp.status, resp.reason))
        print("Error message: %s" % resp.msg.getheader('X-Error-Detail'))
        sys.exit(1)

    data = json.loads(data)
    requestName = data['result'][0]['request']
    print("Clone: %s" % requestName)
    return requestName


if __name__ == "__main__":
    if len(sys.argv) > 3 or len(sys.argv) < 2:
        print("Usage: python clone.y WORKFLOW_NAME OVERRIDE_DICT")
        print("  e.g.: python clone.py pdmvserv_task_HIG-PhaseIIFall16LHEGS82-00018__v1_T_170228_170033_676 '{\"key\": \"alan\"}'")
        sys.exit(0)

    workflow = sys.argv[1]
    override = {}
    if len(sys.argv) == 3:
        override = json.loads(sys.argv[2])
    newWorkflow = createClone(workflow, override)

    sys.exit(0)
