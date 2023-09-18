#!/usr/bin/env python
"""
This script can be used to update the status of a request in ReqMgr2.
"""
from __future__ import print_function, division

import json
import os
import sys
import http.client


def setStatus(url, workflow, newstatus):
    headers = {"Content-type": "application/json",
               "Accept": "application/json"}

    if newstatus in ['closed-out', 'announced', 'aborted', 'rejected']:
        encodedParams = json.dumps({"RequestStatus": newstatus, "cascade": True})
    else:
        encodedParams = json.dumps({"RequestStatus": newstatus})

    conn = http.client.HTTPSConnection(url, cert_file=os.getenv('X509_USER_PROXY'), key_file=os.getenv('X509_USER_PROXY'))
    conn.request("PUT", "/reqmgr2/data/request/%s" % workflow, encodedParams, headers)
    resp = conn.getresponse()
    if resp.status != 200:
        print("Response status: %s\tResponse reason: %s\tError-detail: %s" % (resp.status, resp.reason, resp.getheader("x-error-detail")))
        print("  FAILED status transition for: %s" % workflow)
    else:
        print("  OK!")
    conn.close()


def getStatus(url, workflow):
    headers = {"Content-type": "application/json",
               "Accept": "application/json"}

    conn = http.client.HTTPSConnection(url, cert_file=os.getenv('X509_USER_PROXY'), key_file=os.getenv('X509_USER_PROXY'))
    urn = "/reqmgr2/data/request/%s" % workflow
    conn.request("GET", urn, headers=headers)
    resp = conn.getresponse()
    if resp.status != 200:
        print("Response status: %s\tResponse reason: %s\tError-detail: %s" % (resp.status, resp.reason, resp.msg.getheader("x-error-detail")))
        return None
    else:
        resp = json.loads(resp.read())["result"][0]
    return resp[workflow]['RequestStatus']


def main():
#    url = 'cmsweb.cern.ch'
    url = 'cmsweb-testbed.cern.ch'
#    url = 'alancc7-cloud1.cern.ch'
#    url = 'alancc7-cloud2.cern.ch'
#    url = 'cmsweb-k8s-testbed.cern.ch'
#    url = 'cmsweb-test9.cern.ch'

    args = sys.argv[1:]
    if not len(args) == 2:
        print("usage: python setrequeststatus.py <text_file_with_the_workflow_names> <newStatus>")
        sys.exit(0)
    inputFile = args[0]
    newstatus = args[1]
    with open(inputFile, 'r') as fOjb:
        workflows = fOjb.readlines()

    for wflowName in workflows:
        wflowName = wflowName.rstrip('\n')
        currStatus = getStatus(url, wflowName)
        if not currStatus:
            print("  FAILED to retrieve status for workflow: %s" % wflowName)
            continue
        print("Setting %s status from %s to %s" % (wflowName, currStatus, newstatus))
        setStatus(url, wflowName, newstatus)


if __name__ == "__main__":
    main()
