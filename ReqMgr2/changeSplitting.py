#!/usr/bin/env python

from __future__ import print_function

import os
import sys
import httplib
import json
from pprint import pprint

url = "alan-cloud1.cern.ch"


def changeSplitting(workflowName, newSplitting):
    print("Changing splitting for %s ..." % workflowName)
    encodedParams = json.dumps(newSplitting)
    headers = {"Content-type": "application/json", "Accept": "application/json"}

    conn = httplib.HTTPSConnection(url, cert_file=os.getenv('X509_USER_PROXY'), key_file=os.getenv('X509_USER_PROXY'))
    conn.request("POST", "/reqmgr2/data/splitting/%s" % workflowName, encodedParams, headers)
    resp = conn.getresponse()
    if resp.status != 200:
        print("Response status: %s\tResponse reason: %s" % (resp.status, resp.reason))
        if hasattr(resp.msg, "x-error-detail"):
            print("Error message: %s" % resp.msg["x-error-detail"])
        sys.exit(2)
    conn.close()
    print("Splitting arguments successfully updated!")


def retrieveSplitting(workflowName):
    headers = {"Content-type": "application/json", "Accept": "application/json"}
    conn = httplib.HTTPSConnection(url, cert_file=os.getenv('X509_USER_PROXY'), key_file=os.getenv('X509_USER_PROXY'))
    urn = "/reqmgr2/data/splitting/%s" % workflowName
    conn.request("GET", urn, headers=headers)
    r2 = conn.getresponse()
    splitList = json.loads(r2.read())["result"]

    return splitList


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python changeSplitting.py WORKFLOW_NAME")
        sys.exit(0)

    workflow = sys.argv[1]
    splitSchema = retrieveSplitting(workflow)
    pprint(splitSchema)

    # update max_events_per_lumi only
    newSplit = []
    for task in splitSchema:
        if 'max_events_per_lumi' in task['splitParams']:
            if task['splitParams']['max_events_per_lumi'] == 20000:
                task['splitParams']['max_events_per_lumi'] = 100000
                newSplit.append(task)

    # now we just post the new splitting parameters (only for tasks we had to change)
    # pprint(newSplit)
    # sys.exit(0)
    changeSplitting(workflow, newSplit)

    sys.exit(0)
