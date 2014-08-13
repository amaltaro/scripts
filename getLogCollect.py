#!/usr/bin/env python -u
import sys, os
import httplib, json
import pprint
from optparse import OptionParser

cmsweb_url = 'cmsweb.cern.ch'

def getWorkloadSummary(request):
    """
    Receives the requestName as input and then fetch its workloadsummary
    in couchdb. Returns a json object.
    """
    conn = httplib.HTTPSConnection(cmsweb_url, \
           cert_file = os.getenv('X509_USER_PROXY'), \
           key_file = os.getenv('X509_USER_PROXY'))
    r1 = conn.request('GET', '/couchdb/workloadsummary/' + request)
    r2 = conn.getresponse()
    data = r2.read()
    s = json.loads(data)
    conn.close()
    return s

def main():
    """
    Provided a request name, it will get the workloadSummary and then filter out
    all the logCollect tarball paths. The output will be written to a file.
    """
    usage = "Usage: %prog -r request"
    parser = OptionParser(usage = usage)
    parser.add_option('-r', '--request', help = 'Corresponds to the request name in ReqMgr', dest = 'request')
    (options, args) = parser.parse_args()
    if not options.request:
        parser.error('You must provide a workflow name')
        sys.exit(1)

    reqout = getWorkloadSummary(options.request)
    #pprint.pprint(reqout['logArchives'])

    # will skip Merge logCollects
    f = open('listOfLogCollets.txt', 'w')
    for _, v in reqout['logArchives'].iteritems():
        for tar in v:
            if not 'outputMergeLogCollect' in tar:
                print tar.split('SFN=')[1]
                f.write(tar.split('SFN=')[1]+'\n')

    print '\nList of tarballs saved in listOfLogCollets.txt'
    f.close()
    sys.exit(0)

if __name__ == "__main__":
        main()
