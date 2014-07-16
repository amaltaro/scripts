# /bin/python
from optparse import OptionParser
import urllib, urllib2, httplib
from urllib2 import HTTPError, URLError
from urlparse import urljoin
import json, sys
import pprint

main_url        = "https://cmsweb.cern.ch"
couch_workload  = main_url + "/couchdb/workloadsummary/"
couch_reqmgr    = main_url + "/couchdb/reqmgr_workload_cache/"
phedex_url      = main_url + "/phedex/datasvc/json/prod/"
dbs_url         = main_url + "/dbs/prod/global/DBSReader/"

class HTTPSClientAuthHandler(urllib2.HTTPSHandler):
    def __init__(self, key, cert):
        urllib2.HTTPSHandler.__init__(self)
        self.key = key
        self.cert = cert

    def https_open(self, req):
        return self.do_open(self.getConnection, req)

    def getConnection(self, host, timeout=300):
        return httplib.HTTPSConnection(host, key_file=self.key, cert_file=self.cert)

def ifDict(val_in):
    if isinstance(val_in, dict):
        return True
    return False

def get_content(url, cert, params=None):
    output = ""
    opener = urllib2.build_opener(HTTPSClientAuthHandler(cert, cert) )
    try:
        if params:
            response = opener.open(url, params)
            output = response.read()
        else :
            response = opener.open(url)
            output = response.read()
    except HTTPError as e:
        print 'The server couldn\'t fulfill the request'
        print 'Error code: ', e.code
        sys.exit(1)
    except URLError as e:
        print 'Failed to reach server'
        print 'Reason: ', e.reason
        sys.exit(1)
    return output

def phedex_info(dataset, cert):
    """
    Query blockreplicas PhEDEx API to retrieve detailed information
    for a specific dataset
    """
    phedex_summary= {}
    api_url = phedex_url + "blockreplicas" + "?" + urllib.urlencode([('dataset', dataset)])
    phedex_summary = json.loads(get_content(api_url, cert))
    return phedex_summary

def dbs_info(dataset, cert):
    """
    Queries 2 DBS APIs to get both summary and detailed information
    """
    dbs_out= {}
    api_url = dbs_url + "blocksummaries" + "?" + urllib.urlencode({'dataset' :dataset})
    dbs_out['blocksummaries'] = json.loads(get_content(api_url, cert))
    api_url = dbs_url + "files" + "?" + urllib.urlencode({'dataset' :dataset}) + "&detail=1"
    dbs_out['files'] = json.loads(get_content(api_url, cert))
    return dbs_out

def main(argv=None):
    """
    Receive a dataset name and proxy location and then gets information
    from the following sources:
     - phedex : gets number of files
     - dbs    : gets the number of valid, invalid and total files
    """

    usage = "usage: %prog -d dataset_name -p proxy_location"
    parser = OptionParser(usage=usage)
    parser.add_option('-d', '--dataset', help='Dataset name',dest='dataset')
    parser.add_option('-p', '--proxy', help='Path to your proxy or cert location',dest='proxy')
    (options, args) = parser.parse_args()
    if not (options.dataset and options.proxy):
        parser.error("Please supply dataset name and certificate location")
        sys.exit(1)
    dataset = options.dataset
    cert = options.proxy

    phedex_out = phedex_info(dataset, cert)
    dbs_out = dbs_info(dataset, cert)

    phedex_files = 0
    for item in phedex_out["phedex"]["block"]:
        phedex_files += item['files']

    dbs_files = dbs_out['blocksummaries'][0]['num_file']
    print "Phedex file count : ", phedex_files
    print "DBS file count    : ", dbs_files

    dbs_file_valid = 0
    dbs_file_invalid = 0
    for item in dbs_out['files']:
        if item['is_file_valid']:
            dbs_file_valid += 1
        else:
            dbs_file_invalid += 1
    print " - valid files    : ", dbs_file_valid
    print " - invalid files  : ", dbs_file_invalid
    print " - valid+invalid  : ", (dbs_file_valid + dbs_file_invalid)

if __name__ == "__main__":
    sys.exit(main())
