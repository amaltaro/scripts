#!/usr/bin/env python
# encoding: utf-8
"""
Retrieve data from the ACDC server and get the amount
of failures for a given workflow in a per-task basis.
"""

import sys
from pprint import pprint
from optparse import OptionParser
from WMCore.Database.CMSCouch import Database

def main():
    """
    _main_
    """
    usage = "Usage: python %prog -w workflow"
    parser = OptionParser(usage = usage)
    parser.add_option('-w', '--workflow', help = 'Workflow name in ReqMgr', dest = 'wf')
    (options, args) = parser.parse_args()
    if not options.wf:
        parser.error('You must provide a workflow name')
        sys.exit(1)

    couchUrl = "https://cmsweb.cern.ch/couchdb"
    database = "acdcserver"
    failures = {}
    svc = Database(database, couchUrl)

    result = svc.loadView("ACDC", "byCollectionName", {'key' : options.wf, 'include_docs' : True, 'reduce' : False})
    print "Found %i failures/rows in total." % len(result["rows"])
    for entry in result["rows"]:
        if entry['doc']['fileset_name'] in failures:
            failures[entry['doc']['fileset_name']] += 1
        else:
            failures[entry['doc']['fileset_name']] = 1
    pprint(failures)
    print "\nDone!"

if __name__ == "__main__":
    sys.exit(main())
