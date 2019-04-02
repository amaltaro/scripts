#!/usr/bin/env python
# encoding: utf-8
"""
Retrieve data from the ACDC server and get the amount
of failures for a given workflow in a per-task basis.

You need to source the agent init.sh before running it:
source /data/srv/wmagent/current/sw/slc7_amd64_gcc630/cms/wmagent/1.1.20.patch5/etc/profile.d/init.sh

and example of command line is:
python checkACDCCollections.py -w pdmvserv_task_HIG-RunIIFall17wmLHEGS-03538__v1_T_190222_091942_5061 -v
"""
from __future__ import print_function

import sys
from optparse import OptionParser
from pprint import pprint

from WMCore.Database.CMSCouch import Database


def main():
    """
    _main_
    """
    usage = "Usage: python %prog -w workflow"
    parser = OptionParser(usage=usage)
    parser.add_option('-w', '--workflow', help='Workflow name in ReqMgr', dest='wf')
    parser.add_option('-v', '--verbose', help='Enable verbose mode', action="store_true")
    (options, args) = parser.parse_args()
    if not options.wf:
        parser.error('You must provide a workflow name')
        sys.exit(1)

    couchUrl = "https://cmsweb.cern.ch/couchdb"
    database = "acdcserver"
    failures = {}
    svc = Database(database, couchUrl)

    result = svc.loadView("ACDC", "byCollectionName", {'key': options.wf, 'include_docs': True, 'reduce': False})
    print("Found %i failures/rows in total." % len(result["rows"]))
    for entry in result["rows"]:
        fsetName = entry['doc']['fileset_name']
        failures.setdefault(fsetName, {'jobs': 0, 'files': 0, 'lumis': 0, 'uniqueLumis': 0, 'listLumis': set()})

        failures[fsetName]['jobs'] += 1
        failures[fsetName]['files'] += len(entry['doc']['files'])
        for fname in entry['doc']['files']:
            for runLumi in entry['doc']['files'][fname]['runs']:
                failures[fsetName]['lumis'] += len(runLumi['lumis'])
                failures[fsetName]['listLumis'] |= set(runLumi['lumis'])

    for fsetName in failures:
        failures[fsetName]['uniqueLumis'] = len(failures[fsetName]['listLumis'])
        failures[fsetName]['listLumis'] = str(failures[fsetName]['listLumis'])

    if not options.verbose:
        for fset in failures.keys():
            failures[fset].pop('listLumis', None)

    print("Summary of failures is as follows:")
    pprint(failures)

    print("\nNow printing duplicate files + run + lumis per fileset")
    printDups(result["rows"])
    print("\nDone!")


def printDups(chunkFiles):
    """
    Same logic as mergeFilesInfo from WMCore
    """
    mergedFiles = {}

    # Merge ACDC docs without any real input data (aka MCFakeFile)
    for row in chunkFiles:
        for fname, values in row['doc']['files'].iteritems():
            if fname not in mergedFiles:
                mergedFiles[fname] = {}
                for runLumi in values['runs']:
                    runNum = str(runLumi['run_number'])
                    mergedFiles[fname][runNum] = set(runLumi['lumis'])
            else:
                for runLumi in values['runs']:
                    runNum = str(runLumi['run_number'])
                    lumiSet = set(runLumi['lumis'])
                    if lumiSet.issubset(mergedFiles[fname][runNum]):
                        print("Duplicate file: %s" % values)
                        continue
                    else:
                        mergedFiles[fname][runNum] |= lumiSet


if __name__ == "__main__":
    sys.exit(main())
