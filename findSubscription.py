#!/usr/bin/env python

import urllib2,urllib, httplib, sys, re, os, json, time, locale
import optparse
from xml.dom.minidom import getDOMImplementation

def findSubscription(url, dataset):
    conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
    r1=conn.request("GET",'/phedex/datasvc/json/prod/requestlist?dataset='+dataset)
    r2=conn.getresponse()
    data = r2.read()
    data = json.loads(data)
    requests = data['phedex']['request']
    flag = 1  # means it didn't find any approved transfer to CERN
    for req in requests:
        site = req['node'][0]['name']
        decision = req['node'][0]['decision']
        requestor = req['requested_by']
        type = req['type']
        if site == 'T2_CH_CERN' and decision == 'approved' \
            and type == 'xfer' and 'Alan' in requestor:
            print req['id']
            flag = 0
        elif site == 'T2_CH_CERN' and decision == 'approved' and type == 'xfer':
            print req['id'], requestor
            flag = 0
    if flag:
        print "Transfer request to CERN was not found for", dataset
        sys.exit(1)

def changeRequestGroup(url, dataset, group):
#    group = 'RelVal'
    node = 'T2_CH_CERN'
    params = urllib.urlencode({ "group" : group, "node": node, "dataset" : dataset})
    conn  =  httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
    r1 = conn.request("POST", "/phedex/datasvc/json/prod/updatesubscription", params)
    r2 = conn.getresponse()
    print 'Status:',r2.status
    if r2.status != 200:
        print 'Reason:',r2.reason
    print 'Explanation:',r2.read()

def main():
    url='cmsweb-testbed.cern.ch'    
    parser = optparse.OptionParser()
    parser.add_option('-d', '--dataset', help='Name of the dataset', dest='dataset')
    parser.add_option('-g', '--group', help='Name of the target PhEDEx group', dest='group')
#    parser.add_option('--change',action="store_true", help='Use it to change to RelVal group.',dest='change')
    (options,args) = parser.parse_args()
    if not options.dataset:
        print "Must specify a dataset name"
        print "Usage: python2.6 findSubscription.py -d <dataset> {-g <groupName>}"
        sys.exit(0);
    dataset=options.dataset
    findSubscription(url,dataset)
    if options.group:
        newGroup = options.group
        changeRequestGroup(url, dataset, newGroup)
    sys.exit(0);

if __name__ == "__main__":
    main()
