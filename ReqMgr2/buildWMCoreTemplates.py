#!/usr/bin/env python
"""
Run it from vocms049 with your proxy in the environment
"""

import http.client
import json
import os
import random
import re
import sys
from dbs.apis.dbsClient import DbsApi
from pprint import pformat


def migrateDataset(datasets):
    """
    Migrate dataset from the production to the integration DBS database.
    It returns the origin site name, which should be used for assignment
    """
    dbsApi = DbsApi(url='https://cmsweb-testbed.cern.ch/dbs/int/global/DBSMigrate/')
    dbsInst = "https://cmsweb.cern.ch/dbs/prod/global/DBSReader"

    datasets = list(set(datasets))
    for dset in datasets:
        migrateArgs = {'migration_url': dbsInst, 'migration_input': dset}
        try:
            dbsApi.submitMigration(migrateArgs)
        except Exception as exc:
            errorMsg = str(getattr(exc, "body", str(exc)))
            if "already in destination" in errorMsg:
                print("Dataset {} is already available in int/global DBS".format(dset))
            else:
                raise
        else:
            print("Migrating dataset %s to int/global DBS" % dset)


def findDsets(reqDict):
    """
    Find any datasets present in the request configuration.
    """
    dsets = []
    for k, v in list(reqDict.items()):
        if k in ('MCPileup', 'DataPileup', 'InputDataset'):
            dsets.append(v)
        elif re.match(r"Task[0-9]$", k) or re.match(r"Step[0-9]$", k):
            # it's either TaskChain or Stepchain, check the inner dict then
            dsets.extend(findDsets(v))
    return dsets


def getRequestDict(workflow):
    # url = "cmsweb-testbed.cern.ch"
    url = "cmsweb.cern.ch"
    headers = {"Content-type": "application/json",
               "Accept": "application/json"}
    conn = http.client.HTTPSConnection(url, cert_file=os.getenv('X509_USER_PROXY'),
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
    paramBlacklist = ['AllowOpportunistic', 'AutoApproveSubscriptionSites', 'BlockCloseMaxEvents', 'BlockCloseMaxFiles',
                      'BlockCloseMaxSize',
                      'BlockCloseMaxWaitTime', 'CouchURL', 'CouchWorkloadDBName', 'CustodialGroup', 'CustodialSites',
                      'CustodialSubType',
                      'Dashboard', 'DeleteFromSource', 'GracePeriod', 'Group', 'HardTimeout', 'InitialPriority',
                      'InputDatasets',
                      'MaxMergeEvents', 'MaxMergeSize', 'MaxVSize', 'MergedLFNBase', 'MinMergeSize',
                      'NonCustodialGroup', 'NonCustodialSites', 'NonCustodialSubType', 'OutputDatasets', 'ReqMgr2Only',
                      'RequestDate',
                      'RequestName', 'RequestSizeFiles', 'RequestStatus', 'RequestTransition', 'RequestWorkflow',
                      'RequestorDN', 'SiteWhitelist', 'SoftTimeout', 'SoftwareVersions', 'SubscriptionPriority',
                      'Team', 'Teams', 'TotalEstimatedJobs', 'TotalInputEvents', 'TotalInputFiles', 'TotalInputLumis',
                      'TotalTime',
                      'TrustPUSitelists', 'TrustSitelists', 'UnmergedLFNBase', '_id', 'inputMode', 'timeStamp',
                      'DN', 'DQMHarvestUnit', 'DashboardHost', 'DashboardPort', 'EnableNewStageout', 'FirstEvent',
                      'FirstLumi', 'PeriodicHarvestInterval', 'RobustMerge', 'RunNumber', 'ValidStatus', 'VoGroup',
                      'PriorityTransition',
                      'VoRole', 'dashboardActivity', 'mergedLFNBase', 'unmergedLFNBase', 'MaxWaitTime',
                      'OutputModulesLFNBases', 'Override',
                      'ChainParentageMap', 'OpenRunningTimeout', 'Requestor', 'ParentageResolved']

    createDict = {}
    # print(pformat(reqDict))
    createDict['Comments'] = {"WorkFlowDesc": "", "CheckList": ""}
    if reqDict.get("EnableHarvesting", False):
        createDict['EnableHarvesting'] = reqDict['EnableHarvesting']
        createDict['DQMHarvestUnit'] = reqDict['DQMHarvestUnit']
        createDict['DQMUploadUrl'] = "https://cmsweb-testbed.cern.ch/dqm/dev"

    for key, value in list(reqDict.items()):
        if key in paramBlacklist:
            continue
        elif value in ([], {}, None, ''):
            continue
        elif key == 'Campaign':
            createDict[key] = "Campaign-OVERRIDE-ME"
        elif key == 'RequestString':
            createDict[key] = "RequestString-OVERRIDE-ME"
        elif key == 'DbsUrl':
            createDict[key] = "https://cmsweb-testbed.cern.ch/dbs/int/global/DBSReader/"
        elif key == 'RequestPriority':
            createDict[key] = min(value + 100000, 999999)
        elif key == 'PrepID':
            createDict[key] = 'TEST-' + value
        elif key in ['ConfigCacheURL', 'ConfigCacheUrl']:
            createDict['ConfigCacheUrl'] = value
        else:
            createDict[key] = value

    newSchema = {'createRequest': createDict}
    if createDict['RequestType'] in ['TaskChain', 'StepChain']:
        chainNames = handleTasksSteps(createDict)
    newSchema['assignRequest'] = handleAssignmentParams(createDict, chainNames)
    newSchema['assignRequest']['Override'] = {
        "eos-lfn-prefix": "root://eoscms.cern.ch//eos/cms/store/logs/prod/recent/TESTBED"}

    return newSchema


def handleTasksSteps(reqDict):
    """
    Remove/overwrite some values
    :return: a list of task or step names
    """
    chainNames = []
    number = reqDict.get('TaskChain', reqDict.get('StepChain'))
    name = 'Task' if reqDict['RequestType'] == 'TaskChain' else 'Step'

    # to avoid mismatch of key names, let's set it to a base string
    reqDict['AcquisitionEra'] = "DEFAULT_AcqEra"
    reqDict['ProcessingString'] = "DEFAULT_ProcStr"
    for i in range(1, number + 1):
        thisDict = name + str(i)
        for k in list(reqDict[thisDict].keys()):
            # remove None and empty stuff
            if reqDict[thisDict][k] in ([], {}, None, ''):
                reqDict[thisDict].pop(k)
            elif k == 'ProcessingString':
                reqDict[thisDict][k] = "%s%s_WMCore_TEST" % (name, i)
            # this info is used for assignment, so we better make sure there are no dashes in here
            elif k in ('TaskName', 'StepName'):
                reqDict[thisDict][k] = reqDict[thisDict][k].replace('-', '_')
                chainNames.append(reqDict[thisDict][k].replace('-', '_'))
            elif k in ('InputTask', 'InputStep'):
                reqDict[thisDict][k] = reqDict[thisDict][k].replace('-', '_')
    return chainNames


def handleAssignmentParams(reqDict, chainNames):
    """
    Add some predefined assignment parameters to the template
    :param chainNames: a list of task or step names (or empty for ReReco)
    """
    assignDict = {"SiteWhitelist": ["SiteWhitelist-OVERRIDE-ME"],
                  "Team": "Team-OVERRIDE-ME",
                  "AcquisitionEra": "AcquisitionEra-OVERRIDE-ME",
                  "ProcessingString": "ProcessingString-OVERRIDE-ME",
                  "Dashboard": "Dashboard-OVERRIDE-ME",
                  "ProcessingVersion": 19,
                  "MergedLFNBase": "/store/backfill/1",
                  "UnmergedLFNBase": "/store/unmerged",
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

    if chainNames:
        # then it's either a TaskChain or a StepChain workflow. Tweak assignment
        for k in ['AcquisitionEra', 'ProcessingString']:
            assignDict[k] = {}
            for name in chainNames:
                assignDict[k][name] = k + "-OVERRIDE-ME"

    return assignDict


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
    dsets = findDsets(newDict['createRequest'])
    if dsets:
        try:
            migrateDataset(dsets)
        except Exception as ex:
            errorMsg = str(getattr(ex, "body", str(ex)))
            print("Error migrating dataset between DBS instances. Details: {}".format(errorMsg))
    createJsonTemplate(newDict)


if __name__ == "__main__":
    main()
