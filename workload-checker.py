# /bin/python
# -*- coding: utf-8 -*-
# Author : Justas Balcas justas.balcas@cern.ch
# Date : 2014-01-29
import os, os.path, sys
from optparse import OptionParser
import urllib, urllib2, httplib
from urllib2 import HTTPError, URLError
from urlparse import urljoin
import json
import pprint

# table parameters 
separ_line = "|"+"-"*54+"|"
split_line = "|"+"*"*54+"|"

class HTTPSClientAuthHandler(urllib2.HTTPSHandler):
    def __init__(self, key, cert):
        urllib2.HTTPSHandler.__init__(self)
        self.key = key
        self.cert = cert

    def https_open(self, req):
        # Rather than pass in a reference to a connection class, we pass in
        # a reference to a function which, for all intents and purposes,
        # will behave as a constructor
        return self.do_open(self.getConnection, req)

    def getConnection(self, host, timeout=300):
        return httplib.HTTPSConnection(host, key_file=self.key, cert_file=self.cert)

def ifDict(val_in):
    if isinstance(val_in, dict):
        return True
    return False

def get_content(url, cert, params=None):
    opener = urllib2.build_opener(HTTPSClientAuthHandler(cert, cert) )
    try:
        if params:
            response = opener.open(url, params)
            output = response.read()
        else :
            response = opener.open(url)
            output = response.read()
    except HTTPError as e:
        print 'The server couldn\'t fulfill the request at %s' % url
        print 'Error code: ', e.code
        sys.exit(1)
    except URLError as e:
        print 'Failed to reach server at %s' % url
        print 'Reason: ', e.reason
        sys.exit(1)
    return output   

def reqmgr_outputs(reqName, cert, base_url):
    """
    Queries reqmgr db for the output datasets
    """
    #base_url = "https://reqmgr2-dev.cern.ch"
    reqmgr_url = base_url + "/reqmgr/reqMgr/outputDatasetsByRequestName?requestName=" + reqName
    output_dsets = json.loads(get_content(reqmgr_url, cert))
    return output_dsets

def couch_info(reqName, cert, base_url):
    """
    Queries couchdb wmstats database for the workloadSummary
    """
    #base_url = "https://reqmgr2-dev.cern.ch"
    couch_output = {}
    couch_workl = urljoin(base_url+"/couchdb/workloadsummary/", reqName)
    couch_output["workflow_summary"] = json.loads(get_content(couch_workl, cert))
    return couch_output

def reqmgr_info(reqName, cert, base_url):
    """
    Queries Request Manager database for the spec file and a few other stuff
    """
    #base_url = "https://reqmgr2-dev.cern.ch"
    reqmgr_output = {}
    couch_reqmgr = urljoin(base_url+"/couchdb/reqmgr_workload_cache/", reqName)
    reqmgr_output["reqmgr_input"] = json.loads(get_content(couch_reqmgr, cert))
    return reqmgr_output

def phedex_info(datasets, cert, base_url):
    """
    Queries blockreplicas PhEDEx API to retrieve general information needed
    """
    phedex_summary= {}
    phedex_query_params = urllib.urlencode([('dataset', i) for i in datasets])
    phedex_url = urljoin(base_url+"/phedex/datasvc/json/prod/", "blockreplicas")
    phedex_summary["blockreplicas"] = json.loads(get_content(phedex_url, cert, phedex_query_params))
    return phedex_summary 

def dbs_info(dataset, cert, base_url):
    """
    Queries 3 DBS APIs to get all general information needed
    """
    if 'testbed' in base_url:
        dbs_url = base_url + "/dbs/int/global/DBSReader/"
    else:
        dbs_url = base_url + "/dbs/prod/global/DBSReader/"

    dbs_out= {}
    dbs_out[dataset] = {"blocksummaries": [], "blockorigin": [], "filesummaries": []}
    for api in dbs_out[dataset]:
        full_url = dbs_url + api + "?" + urllib.urlencode({'dataset': dataset})
        data = json.loads(get_content(full_url, cert))     
        dbs_out[dataset][api] = data
    # Separate query for prep_id, since we want any access_type 
    full_url = dbs_url + "datasets?" + urllib.urlencode({'dataset': dataset})
    full_url += "&dataset_access_type=*&detail=True"
    data = json.loads(get_content(full_url, cert))
    # if dataset is not available in DBS ...
    dbs_out[dataset]["prep_id"] = data[0]["prep_id"] if data else None

    return dbs_out 

def list_cmp(d1, d2, d3 = []):
    """
    Receives list of things (datasets, events, sizes) and compare them.
    """
    if isinstance(d1, list): 
        if not d3:
            if set(d1) ^ set(d2):
                return 'NOPE'
        else:
            if set(d1) ^ set(d2) or set(d1) ^ set(d3):
                return 'NOPE'
    else:
        if not d3:
            if set(d1.values()) ^ set(d2.values()):
                return 'NOPE'
        else:
            if set(d1.values()) ^ set(d2.values()) or set(d1.values()) ^ set(d3.values()):
                return 'NOPE'
    return 'ok'

def couch_verbose(couch_input, couch_summary, verbose=False):
    """
    Print CouchDB and ReqMgr information retrieved for the input in
    the spec file and  output samples information
    """
    print '\n' + '*' * 44 + ' Couch info ' + '*' * 45
    if 'RequestNumEvents' in couch_input:
        print 'RequestNumEvents :', couch_input['RequestNumEvents']
    else:
        print 'Input Dataset   :', couch_input['InputDataset']
        print '- Estimated jobs:', couch_input['TotalEstimatedJobs']
        print '- Input Events  :', couch_input['TotalInputEvents']
        print '- Input Lumis   :', couch_input['TotalInputLumis']
        print '- Input Files   :', couch_input['TotalInputFiles']
    print '-'*101
    if not couch_summary:
        print "Couch output workload summary is empty!"
        return
    for dset,value in couch_summary.iteritems():
        print 'Output Dataset:', dset
        print '- Num events  :', value['events']
        print '- Num files   :', value['dset_files']
        print '- Dset size   :', value["dset_size"]

def phedex_verbose(phedex_out, phedex_summary, verbose=False):
    """
    Print PhEDEx information retrieved for the output samples
    and print them so one can x-check and debug if needed.
    """
    print '\n' + '*' * 44 + ' PhEDEx info ' + '*' * 44
    for key,value in phedex_summary.iteritems():
        print 'Dataset Name :', key
        print '- Dset size  :', value[0]["dset_size"]
        print '- Num blocks :', value[0]["num_block"]
        print '- Dset files :', value[0]["dset_files"]
        for item in value:
            print '- Block name :', item["block"]
            print ' - Num Files :', item["files"]
            print ' - Block size:', item["bytes"]
            print ' - Is Open   :', item["is_open"]
            print ' - Site      :', item["node"]
            print ' - Custodial :', item["custodial"]
            print ' - Complete  :', item["complete"]
            print ' - Subscribed:', item["subscribed"]
    # TODO: not sure we want this replica verbose thing
    if verbose:
        print '\n' + '*' * 44 + ' Replica info ' + '*' * 44
        for item in phedex_out["blockreplicas"]["phedex"]["block"]:
            print 'Rpl Name     : ', item["name"]
            print 'Rpl files    : ', item["files"]
            print 'Rpl bytes    : ', item["bytes"]
            print 'Rpl open     : ', item["is_open"]

def dbs_verbose(dbs_summary, verbose=False):
    """
    Print DBS information retrieved for the output samples
    and print them so one can x-check and debug if needed.
    """
    print '\n' + '*' * 45 + ' DBS3 info ' + '*' * 45
    for key,value in dbs_summary.iteritems():
        if value:
            print 'Dataset Name :', key
            print '- PrepID     :', value[0]["prep_id"]
            print '- Dset size  :', value[0]["dset_size"]
            print '- Num blocks :', value[0]["num_block"]
            print '- Dset files :', value[0]["dset_files"]
            print '- Num events :', value[0]["num_event"]
            print '- Num lumi   :', value[0]["num_lumi"]
            for item in value:
                print '- Block name :', item["block_name"]
                print ' - Num Files :', item["file_count"]
                print ' - Block size:', item["block_size"]
                print ' - Is Open   :', item["open_for_writing"]
                print ' - Origin SE :', item["se"]

def main(argv=None):
    """
    Receive a workflow name and proxy location and then gets information
    from the following sources:
     - couchdb: gets the output datasets and spec file
     - phedex: gets dataset, block, files information
     - dbs: gets dataset, block and files information
    """

    usage = "usage: %prog -r request_name -p proxy_location [-v|--verbose] [-c|--cms cmsweb_url]"
    parser = OptionParser(usage=usage)
    parser.add_option('-r', '--request', help='Request Name as it is in WMStats',dest='request')
    parser.add_option('-p', '--proxy', help='Path to your proxy or cert location',dest='proxy')
    parser.add_option('-v', '--verbose', action="store_true", help='Set verbose to print nice info for all 3 services.',dest='verbose')
    parser.add_option('-c', '--cms', help='Override DBS3 URL. Example: cmsweb.cern.ch',dest='cms')
    parser.add_option('-m', '--reqmgr', help='Request Manager URL. Example: couch-dev1.cern.ch',dest='reqmgr')
    (options, args) = parser.parse_args()
    if not (options.request and options.proxy):
        parser.error("Please supply request name and certificate location")
        sys.exit(1)
    reqName = options.request
    cert = options.proxy
#    verbose = False
#    if options.verbose:
#        verbose = True

    verbose = True if options.verbose else False

    cmsweb_url = "https://" + options.cms if options.cms else "https://cmsweb-testbed.cern.ch" 
#    if options.cms:
#        cmsweb_url = "https://" + options.cms
#    else:
#        cmsweb_url = "https://cmsweb-testbed.cern.ch"

    reqmgr_url = "https://" + options.reqmgr if options.reqmgr else "https://cmsweb-testbed.cern.ch" 
#    if options.reqmgr:
#        reqmgr_url = "https://" + options.reqmgr
#    else:
#        reqmgr_url = "https://cmsweb-testbed.cern.ch"

    ### Retrieve ReqMgr information
    # TODO: this information may not be available for all type of requests
    print reqName
    reqmgr_out = reqmgr_info(reqName, cert, reqmgr_url)
    try:
        couch_input = {'RequestNumEvents' : reqmgr_out['RequestNumEvents']}
#        print "DEBUG: Got it in try 1"
    except KeyError:
        pass
    try:
        couch_input = {'RequestNumEvents' : reqmgr_out['Task1']['RequestNumEvents']}
#        print "DEBUG: Got it in try 2 (inside Task1)"
    except KeyError:
        pass
    try:
        couch_input = {'TotalEstimatedJobs' : reqmgr_out['reqmgr_input']['TotalEstimatedJobs'],
                       'TotalInputEvents' : reqmgr_out['reqmgr_input']['TotalInputEvents'],
                       'TotalInputLumis' : reqmgr_out['reqmgr_input']['TotalInputLumis'],
                       'TotalInputFiles' : reqmgr_out['reqmgr_input']['TotalInputFiles'],
                       'InputDataset' : ''}
    except KeyError:
        raise AttributeError("Reqmgr_out does not have such attribute")

    ### Retrieve CouchDB information
    # Get workload summary,  output samples, num of files and events
    couch_out = couch_info(reqName, cert, reqmgr_url)
    couch_datasets = reqmgr_outputs(reqName, cert, reqmgr_url)
#    reqmgr_outputs
    couch_num_files = {}
    couch_num_events = {}
    couch_dset_size = {}
    couch_summary = {}
    # check if merge jobs failed
    if couch_out["workflow_summary"]["output"]:
        for dset in couch_datasets:
            if dset in couch_out["workflow_summary"]["output"]:
                couch_num_files[dset] = couch_out["workflow_summary"]["output"][dset]['nFiles']
                couch_num_events[dset] = couch_out["workflow_summary"]["output"][dset]['events']
                couch_dset_size[dset] = couch_out["workflow_summary"]["output"][dset]["size"]
                couch_summary[dset] = {'events' : couch_out["workflow_summary"]["output"][dset]["events"],
                                       'dset_files' : couch_out["workflow_summary"]["output"][dset]["nFiles"],
                                       'dset_size' : couch_out["workflow_summary"]["output"][dset]["size"]}
            else:
                couch_num_files[dset], couch_num_events[dset], couch_dset_size[dset] = None, None, None
                couch_summary[dset] = {'events': None, 'dset_files': None, 'dset_size': None}

    # Complement the couch_input dict with the inputDset, if exists
    if 'inputdatasets' in couch_out['workflow_summary'] and \
      'InputDataset' in couch_input and len(couch_out['workflow_summary']['inputdatasets']) > 0:
        couch_input['InputDataset'] = couch_out['workflow_summary']['inputdatasets'][0] 

    ### Retrieve PhEDEx information
    # Get general phedex info, number of files and size of dataset
    phedex_out = phedex_info(couch_datasets, cert, cmsweb_url)
    phedex_datasets = [i['name'].split('#')[0] for i in phedex_out["blockreplicas"]["phedex"]["block"]]
    phedex_datasets = couch_datasets
    phedex_datasets = list(set(phedex_datasets))
    phedex_num_files = {}
    phedex_dset_size = {}
    phedex_block_se = {}
    phedex_summary = {}
    for item in phedex_out["blockreplicas"]["phedex"]["block"]:
        dset = item['name'].split('#')[0]
        phedex_block_se[item['name']] = item['replica'][0]['se']
        if dset not in phedex_summary:
            phedex_summary[dset] = [{'block' : item['name'],
                                         'files' : item['files'],
                                         'bytes' : item['bytes'],
                                         'is_open' : item['is_open'],
                                         'node' : item['replica'][0]['node'],
                                         'se' : item['replica'][0]['se'],
                                         'custodial' : item['replica'][0]['custodial'],
                                         'complete' : item['replica'][0]['complete'],
                                         'subscribed' : item['replica'][0]['subscribed'],
                                         'dset_size' : item['bytes'],
                                         'num_block' : 1,
                                         'dset_files' : item['files']}]
        else:
            phedex_summary[dset].append({'block' : item['name'],
                                         'files' : item['files'],
                                         'bytes' : item['bytes'],
                                         'is_open' : item['is_open'],
                                         'node' : item['replica'][0]['node'],
                                         'se' : item['replica'][0]['se'],
                                         'custodial' : item['replica'][0]['custodial'],
                                         'complete' : item['replica'][0]['complete'],
                                         'subscribed' : item['replica'][0]['subscribed']})
            phedex_summary[dset][0]['dset_size'] += item['bytes']
            phedex_summary[dset][0]['num_block'] += 1
            phedex_summary[dset][0]['dset_files'] += item['files']
    for dset in phedex_datasets:
        if dset in phedex_summary:
            phedex_num_files[dset] = phedex_summary[dset][0]['dset_files']
            phedex_dset_size[dset] = phedex_summary[dset][0]['dset_size']
        else:
            phedex_num_files[dset] = None
            phedex_dset_size[dset] = None

    ### Retrieve DBS information
    # Starts with a dset list, gets dset_size, dset_events, num_files
    dbs_datasets = list(couch_datasets)
    dbs_out = {}
    dbs_num_files = {}
    dbs_num_events = {}
    dbs_num_lumis = {}
    dbs_dset_size = {}
    dbs_block_se = {}
    dbs_summary = {}
    for dset in dbs_datasets:
        dbs_out = dbs_info(dset, cert, cmsweb_url)
        for item in dbs_out[dset]['blockorigin']:
            dbs_block_se[item['block_name']] = item['origin_site_name']
            if dset not in dbs_summary:
                dbs_summary[dset] = [{'block_name' : item['block_name'],
                                         'file_count' : item['file_count'],
                                         'block_size' : item['block_size'],
                                         'open_for_writing' : item['open_for_writing'],
                                         'se' : item['origin_site_name'],
                                         'dset_size' : 0,
                                         'num_block' : 0,
                                         'dset_files' : 0,
                                         'num_event' : 0,
                                         'num_lumi' : 0}]
            else:
                dbs_summary[dset].append({'block_name' : item['block_name'],
                                         'file_count' : item['file_count'],
                                         'block_size' : item['block_size'],
                                         'open_for_writing' : item['open_for_writing'],
                                         'se' : item['origin_site_name']})
        for item in dbs_out[dset]['blocksummaries']:
            dbs_num_files[dset] = item['num_file']
            dbs_num_events[dset] = item['num_event']
            dbs_dset_size[dset] = item['file_size']
        for item in dbs_out[dset]['filesummaries']:
            dbs_summary[dset][0]['dset_size'] += item['file_size'] 
            dbs_summary[dset][0]['num_block'] += item['num_block'] 
            dbs_summary[dset][0]['dset_files'] += item['num_file'] 
            dbs_summary[dset][0]['num_event'] += item['num_event'] 
            dbs_summary[dset][0]['num_lumi'] += item['num_lumi'] 
        # temp fix when data is not in DBS
        if dset not in dbs_summary:
            dbs_summary[dset] = []
            dbs_num_lumis[dset] = 0
        else:
            dbs_summary[dset][0]['prep_id'] = dbs_out[dset]['prep_id']
            dbs_num_lumis[dset] = dbs_summary[dset][0]['num_lumi']

    ### Perform the FINAL checks
    print ''+split_line 
    print '|' + ' ' * 28 + '| CouchDB | PhEDEx | DBS  |'
    print separ_line

    # Perform checks among the 3 services: dset_name, dset_files and dset_size
    comp_res = list_cmp(couch_summary.keys(), phedex_summary.keys(), dbs_summary.keys())
    print '| Same dataset name          | {comp_res:7s} | {comp_res:6s} | {comp_res:4s} |'.format(comp_res=comp_res)
    comp_res = list_cmp(couch_num_files, phedex_num_files, dbs_num_files)
    print '| Same number of files       | {comp_res:7s} | {comp_res:6s} | {comp_res:4s} | '.format(comp_res=comp_res)
    comp_res = list_cmp(couch_dset_size, phedex_dset_size, dbs_dset_size)
    print '| Same dataset size          | {comp_res:7s} | {comp_res:6s} | {comp_res:4s} | '.format(comp_res=comp_res)

    # Perform check between Couch and DBS only: num_events
    comp_res = list_cmp(couch_num_events, dbs_num_events)
    print '| Same number of events      | %-7s | %-6s | %-4s |' % (comp_res, '--', comp_res)

    comp_res = list_cmp([couch_input['TotalInputLumis']], list(set(dbs_num_lumis.values())))
    print '| Same number of lumis       | %-7s | %-6s | %-4s |' % (comp_res, '--', comp_res)

    # Check whether all blocks are closed
    phe_res, dbs_res = 'ok', 'ok'
    for dset in couch_datasets:
        if dset in phedex_summary:
            for b in phedex_summary[dset]:
                if b['is_open'] == 'y':
                    phe_res = 'NOPE'
                    break
        if dset in dbs_summary:
            for b in dbs_summary[dset]:
                if b['open_for_writing'] == 1:
                    dbs_res = 'NOPE'
                    break

    print '| Are all blocks closed?     | %-7s | %-6s | %-4s |' % ('--', phe_res, dbs_res)

    # Check whether files are registered in the same SE
    comp_res = list_cmp(phedex_block_se, dbs_block_se)
    print '| Are blocks in the same SE? | %-7s | %-6s | %-4s |' % ('--', comp_res, comp_res)
    print split_line

    ### Starts VERBOSE mode for the information retrieved so far
    if verbose:
        couch_verbose(couch_input, couch_summary, False)
        #dbs_verbose(dbs_out, dbs_summary, False)
        dbs_verbose(dbs_summary, False)
        phedex_verbose(phedex_out, phedex_summary, False)

if __name__ == "__main__":
    sys.exit(main())
