# /bin/python
from optparse import OptionParser
import urllib, urllib2, httplib
from urllib2 import HTTPError, URLError
from urlparse import urljoin
import json, sys
import pprint

phedex_url = "https://cmsweb.cern.ch/phedex/datasvc/json/prod/"

class HTTPSClientAuthHandler(urllib2.HTTPSHandler):
    def __init__(self, key, cert):
        urllib2.HTTPSHandler.__init__(self)
        self.key = key
        self.cert = cert

    def https_open(self, req):
        return self.do_open(self.getConnection, req)

    def getConnection(self, host, timeout=300):
        return httplib.HTTPSConnection(host, key_file=self.key, cert_file=self.cert)

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

def phedex_info(block, cert):
    """
    Query blockreplicas PhEDEx API to retrieve detailed information
    for a specific block
    """
    phedex_summary= {}
    api_url = phedex_url + "blockreplicas" + "?" + urllib.urlencode([('block', block)])
    phedex_summary = json.loads(get_content(api_url, cert))
    return phedex_summary

def main(argv=None):
    """
    Receive an input text file with dataset block names and the
    location of the user proxy. It queries each of them to check
     whether they are open or closed in PhEDEx. 

    Open blocks are printed out.
    """

    args=sys.argv[1:]
    if not len(args)==2:
        print "usage: python checkBlockClosed.py <text_file_with_block_names> <proxy_location>"
        sys.exit(0)
    inputFile = args[0]
    proxy = args[1]

    with open(inputFile) as fp:
        for line in fp:
            block = line.rstrip('\n')
            phedex_out = phedex_info(block, proxy)
            #print phedex_out
            if not phedex_out["phedex"]["block"]:
                print "Unavailable %s" % block
            elif phedex_out["phedex"]["block"][0]["is_open"] == 'y':
                print block

if __name__ == "__main__":
    sys.exit(main())
