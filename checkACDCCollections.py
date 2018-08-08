#!/usr/bin/env python
# encoding: utf-8
"""
Retrieve data from the ACDC server and get the amount
of failures for a given workflow in a per-task basis.
"""

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
    (options, args) = parser.parse_args()
    if not options.wf:
        parser.error('You must provide a workflow name')
        sys.exit(1)

    couchUrl = "https://cmsweb.cern.ch/couchdb"
    database = "acdcserver"
    failures = {}
    svc = Database(database, couchUrl)

    result = svc.loadView("ACDC", "byCollectionName", {'key': options.wf, 'include_docs': True, 'reduce': False})
    print "Found %i failures/rows in total." % len(result["rows"])
    for entry in result["rows"]:
        fsetName = entry['doc']['fileset_name']
        failures.setdefault(fsetName, {'jobs': 0, 'files': 0, 'filesAtCERN': 0})

        failures[fsetName]['jobs'] += 1
        failures[fsetName]['files'] += len(entry['doc']['files'])

        for f, v in entry['doc']['files'].iteritems():
            if "T2_CH_CERN" in v['locations']:
                failures[fsetName]['filesAtCERN'] += 1

    pprint(failures)
    print "\nDone!"


if __name__ == "__main__":
    sys.exit(main())
