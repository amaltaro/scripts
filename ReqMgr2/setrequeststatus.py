#!/usr/bin/env python
import sys
import os
import re
import json
import httplib


def setStatus(url, workflow, newstatus):
    headers = {"Content-type": "application/json",
               "Accept": "application/json"}

    print "Setting %s to %s" % (workflow, newstatus)
    if newstatus in ['closed-out', 'announced', 'aborted', 'rejected']:
        encodedParams = json.dumps({"RequestStatus": newstatus, "cascade": True})
    else:
        encodedParams = json.dumps({"RequestStatus": newstatus})

    conn = httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
    conn.request("PUT",  "/reqmgr2/data/request/%s" % workflow, encodedParams, headers)
    resp = conn.getresponse()
    data = resp.read()
    #print data
    if resp.status != 200:
        print "Response status: %s\tResponse reason: %s" % (resp.status, resp.reason)
        if hasattr(resp.msg, "x-error-detail"):
            print "Error message: %s" % resp.msg["x-error-detail"]
            sys.exit(2)
    else:
        print "  OK!"
    conn.close()


def getStatus(url, workflow):
    headers = {"Content-type": "application/json",
               "Accept": "application/json"}

    conn = httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
    urn = "/reqmgr2/data/request/%s" % workflow
    r1=conn.request("GET", urn, headers=headers)
    r2=conn.getresponse()
    request = json.loads(r2.read())["result"][0]
    return request[workflow]['RequestStatus']


def main():
#    url = 'cmsweb.cern.ch'
#    url = 'cmsweb-testbed.cern.ch'
    url = 'alan-cloud1.cern.ch'

    args=sys.argv[1:]
    if not len(args)==2:
        print "usage: python setrequeststatus.py <text_file_with_the_workflow_names> <newStatus>"
        sys.exit(0)
    inputFile = args[0]
    newstatus = args[1]
    f = open(inputFile, 'r')

    for line in f:
        workflow = line.rstrip('\n')
        print("%s" % workflow)
        print "Set %s from %s to %s" % (workflow,getStatus(url, workflow),newstatus)
        setStatus(url, workflow, newstatus)
#        print "Final status is: %s"  % getStatus(url, workflow)
    f.close

if __name__ == "__main__":
    main()
