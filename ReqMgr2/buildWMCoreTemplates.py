#!/usr/bin/env python
import sys
import os
import random
import json
import httplib
from pprint import pprint


def getRequestDict(workflow):
    url = "cmsweb.cern.ch"
    headers = {"Content-type": "application/json",
               "Accept": "application/json"}
    conn = httplib.HTTPSConnection(url, cert_file=os.getenv('X509_USER_PROXY'),
                                   key_file=os.getenv('X509_USER_PROXY'))
    urn = "/reqmgr2/data/request/%s" % workflow
    conn.request("GET", urn, headers=headers)
    r2 = conn.getresponse()
    request = json.loads(r2.read())["result"][0]
    return request[workflow]


def updateRequestDict(reqDict):
    """
    Remove some keys from the original dict and build the
    structure expected by the reqmgr client script.
    """
    paramBlacklist = ['_id', 'AllowOpportunistic', 'BlockCloseMaxEvents', 'BlockCloseMaxFiles', 'BlockCloseMaxSize',
                      'BlockCloseMaxWaitTime', 'CouchURL', 'CouchWorkloadDBName', 'CustodialGroup', 'CustodialSubType',
                      'Dashboard', 'DeleteFromSource', 'GracePeriod', 'HardTimeout', 'InitialPriority', 'InputDatasets',
                      'inputMode', 'MaxMergeEvents', 'MaxMergeSize', 'MaxRSS', 'MaxVSize', 'MinMergeSize',
                      'NonCustodialGroup', 'NonCustodialSubType', 'OutputDatasets', 'ReqMgr2Only', 'RequestDate',
                      'RequestSizeFiles', 'RequestorDN', 'RequestName', 'RequestStatus', 'RequestTransition',
                      'RequestWorkflow', 'SiteWhitelist', 'SoftTimeout', 'SoftwareVersions', 'SubscriptionPriority',
                      'Team', 'Teams', 'timeStamp', 'TrustSitelists', 'TrustPUSitelists', 'TotalEstimatedJobs',
                      'TotalInputEvents', 'TotalInputLumis', 'TotalInputFiles']

    newSchema = {}
    for key, value in reqDict.items():
        if key in paramBlacklist or value in ([], {}, None, ''):
            continue
        elif key == 'OpenRunningTimeout':
            continue
        elif key == 'Campaign':
            newSchema[key] = "Campaign-OVERRIDE-ME"
        elif key == 'RequestString':
            newSchema[key] = "RequestString-OVERRIDE-ME"
        elif key == 'DQMUploadUrl':
            newSchema[key] = "https://cmsweb-testbed.cern.ch/dqm/dev;https://cmsweb.cern.ch/dqm/relval-test"
        elif key == 'DbsUrl':
            newSchema[key] = "https://cmsweb-testbed.cern.ch/dbs/int/global/DBSReader/"
        elif key == 'MergedLFNBase':
            newSchema['MergedLFNBase'] = "/store/backfill/1"
        elif key == 'RequestPriority':
            newSchema[key] = min(value + 100000, 999999)
        elif key == 'PrepID':
            newSchema[key] = 'TEST-' + value
        else:
            newSchema[key] = value

    newSchema['Comments'] = ""
    newSchema['Requestor'] = "amaltaro"
    newSchema['Group'] = "DATAOPS"
    newSchema = {'createRequest': newSchema}

    # add assignment parameters
    _addAssignmentParams(newSchema)

    return newSchema


def _addAssignmentParams(reqDict):
    """
    Add some predefined assignment parameters to the template
    """
    reqDict['assignRequest'] = {"SiteWhitelist": ["SiteWhitelist-OVERRIDE-ME"],
                                "Team": "Team-OVERRIDE-ME",
                                "AcquisitionEra": "AcquisitionEra-OVERRIDE-ME",
                                "ProcessingString": "ProcessingString-OVERRIDE-ME",
                                "Dashboard": "Dashboard-OVERRIDE-ME",
                                "ProcessingVersion": 9,
                                "MergedLFNBase": "/store/backfill/1",
                                "UnmergedLFNBase": "/store/unmerged",
                                "MaxRSS": reqDict['createRequest']['Memory'] * 1000,
                                "MaxVSize": reqDict['createRequest'].get('Multicore', 1) * 20000000,
                                "SoftTimeout": 129600,
                                "GracePeriod": 300,
                                "SiteBlacklist": [],
                                #                                "TrustSitelists": False,
                                #                                "TrustPUSitelists": False,
                                #                                "MinMergeSize": 2147483648,
                                #                                "MaxMergeSize": 4294967296,
                                #                                "MaxMergeEvents": 50000,
                                #                                "CustodialSites": [],
                                #                                "NonCustodialSites": [],
                                #                                "AutoApproveSubscriptionSites": [],
                                #                                "SubscriptionPriority": "Low",
                                #                                "CustodialSubType": "Move",
                                #                                "BlockCloseMaxWaitTime": 14400,
                                #                                "BlockCloseMaxFiles": 500,
                                #                                "BlockCloseMaxEvents": 200000000,
                                #                                "BlockCloseMaxSize": 5000000000000
                                }
    return


def createJsonTemplate(reqDict):
    """
    Create a json file based on the request type and a
    pseudo-random integer, just to avoid dups...
    """
    aNumber = str(random.randint(1, 1000))
    fileName = reqDict['createRequest']['RequestType'] + '_' + aNumber + '.json'
    with open(fileName, 'w') as outFile:
        json.dump(reqDict, outFile, indent=4, sort_keys=True)
    print("File %s successfully created." % fileName)


def main():
    if len(sys.argv) != 2:
        print("You must provide a request name")
        sys.exit(1)

    reqName = sys.argv[1]
    origDict = getRequestDict(reqName)
    newDict = updateRequestDict(origDict)
    createJsonTemplate(newDict)


if __name__ == "__main__":
    main()
