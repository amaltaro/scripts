#!/usr/bin/env python
import os
import sys
import urllib
import urllib2
import httplib
import json
from optparse import OptionParser
from urllib2 import HTTPError, URLError
from urlparse import urljoin
from pprint import pprint

# table parameters 
separ_line = "|" + "-" * 51 + "|"
split_line = "|" + "*" * 51 + "|"

# ID for the User-Agent
CLIENT_ID = 'workload-checker/1.1::python/%s.%s' % sys.version_info[:2]


class HTTPSClientAuthHandler(urllib2.HTTPSHandler):
    """
    Basic HTTPS class
    """

    def __init__(self, key, cert):
        urllib2.HTTPSHandler.__init__(self)
        self.key = key
        self.cert = cert

    def https_open(self, req):
        # Rather than pass in a reference to a connection class, we pass in
        # a reference to a function which, for all intents and purposes,
        # will behave as a constructor
        return self.do_open(self.getConnection, req)

    def getConnection(self, host, timeout=290):
        return httplib.HTTPSConnection(host, key_file=self.key, cert_file=self.cert)


def x509():
    "Helper function to get x509 from env or tmp file"
    proxy = os.environ.get('X509_USER_PROXY', '')
    if not proxy:
        proxy = '/tmp/x509up_u%s' % pwd.getpwuid(os.getuid()).pw_uid
        if not os.path.isfile(proxy):
            return ''
    return proxy


def ifDict(val_in):
    if isinstance(val_in, dict):
        return True
    return False


def get_content(url, params=None):
    cert = x509()
    client = '%s (%s)' % (CLIENT_ID, os.environ.get('USER', ''))
    opener = urllib2.build_opener(HTTPSClientAuthHandler(cert, cert))
    opener.addheaders = [("User-Agent", client)]
    try:
        response = opener.open(url, params)
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


def reqmgr_outputs(reqName, base_url):
    """
    Queries reqmgr db for the output datasets
    """
    reqmgr_url = base_url + "/reqmgr/reqMgr/outputDatasetsByRequestName?requestName=" + reqName
    output_dsets = json.loads(get_content(reqmgr_url))
    return output_dsets


def couch_info(reqName, base_url):
    """
    Queries couchdb wmstats database for the workloadSummary
    """
    couch_output = {}
    couch_workl = urljoin(base_url + "/couchdb/workloadsummary/", reqName)
    couch_output["workflow_summary"] = json.loads(get_content(couch_workl))
    return couch_output


def reqmgr_info(reqName, base_url, reqmgr2):
    """
    Queries Request Manager database for the spec file and a few other stuff
    """
    couch_reqmgr = urljoin(base_url + "/couchdb/reqmgr_workload_cache/", reqName)
    reqmgr_output = json.loads(get_content(couch_reqmgr))
    return reqmgr_output


def phedex_info(datasets, base_url):
    """
    Queries blockreplicas PhEDEx API to retrieve general information needed
    """
    phedex_summary = {}
    phedex_query_params = urllib.urlencode([('dataset', i) for i in datasets])
    phedex_url = urljoin(base_url + "/phedex/datasvc/json/prod/", "blockreplicas")
    phedex_summary["blockreplicas"] = json.loads(get_content(phedex_url, phedex_query_params))
    return phedex_summary


def dbs_info(dataset, base_url):
    """
    Queries 3 DBS APIs to get all general information needed
    """
    if 'testbed' in base_url:
        dbs_url = base_url + "/dbs/int/global/DBSReader/"
    else:
        dbs_url = base_url + "/dbs/prod/global/DBSReader/"

    dbs_out = {}
    dbs_out[dataset] = {"blocksummaries": [], "blockorigin": [], "filesummaries": []}
    for api in dbs_out[dataset]:
        full_url = dbs_url + api + "?" + urllib.urlencode({'dataset': dataset})
        data = json.loads(get_content(full_url))
        dbs_out[dataset][api] = data
    # Separate query for prep_id, since we want any access_type 
    full_url = dbs_url + "datasets?" + urllib.urlencode({'dataset': dataset})
    full_url += "&dataset_access_type=*&detail=True"
    data = json.loads(get_content(full_url))
    # if dataset is not available in DBS ...
    dbs_out[dataset]["prep_id"] = data[0]["prep_id"] if data else ''

    return dbs_out


def dqm_gui(url):
    """
    Fetch all the data from the DQMGui
    """
    dqmgui_url = url + "/data/json/samples"
    dqmgui_out = json.loads(get_content(dqmgui_url))
    return dqmgui_out['samples']


def harvesting(workload, out_dsets):
    """
    Parse the request spec and query DQMGui in case harvesting is enabled
    """
    if workload['RequestType'] == 'DQMHarvest':
        good_output = workload['InputDatasets']
    elif str(workload.get('EnableHarvesting', 'False')) == 'True':
        good_output = [dset for dset in out_dsets if dset.endswith('/DQMIO') or dset.endswith('/DQM')]
    else:
        return

    urls = workload['DQMUploadUrl'].split(';')
    if not good_output:
        print "Well, it's embarassing! Harvesting is enabled but there is nothing to harvest"
        return

    for url in urls:
        print "Harvesting enabled. Querying DQMGui at: %s" % url
        all_samples = dqm_gui(url)
        for out_dset in good_output:
            for sample in all_samples:
                for item in sample['items']:
                    if out_dset == item['dataset']:
                        print item  
        

def list_cmp(d1, d2, d3=[]):
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
    print '*' * 44 + ' Couch info ' + '*' * 45
    print '- Input Dataset     :', couch_input['InputDataset']
    print ' - RequestNumEvents :', couch_input['RequestNumEvents']
    print ' - Input Events     :', couch_input['TotalInputEvents']
    print ' - Estimated jobs   :', couch_input['TotalEstimatedJobs']
    print ' - Input Lumis      :', couch_input['TotalInputLumis']
    print ' - Input Files      :', couch_input['TotalInputFiles']
    print '-' * 101
    if not couch_summary:
        print "Couch output workload summary is empty!"
        return
    for dset, value in couch_summary.iteritems():
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
    for key, value in phedex_summary.iteritems():
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
            print ' - SE        :', item["se"]
            print ' - Custodial :', item["custodial"]
            print ' - Complete  :', item["complete"]
            print ' - Subscribed:', item["subscribed"]


def dbs_verbose(dbs_summary, verbose=False):
    """
    Print DBS information retrieved for the output samples
    and print them so one can x-check and debug if needed.
    """
    print '\n' + '*' * 45 + ' DBS3 info ' + '*' * 45
    for key, value in dbs_summary.iteritems():
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


def main():
    """
    Requirements: you need to have your proxy and proper x509 environment
     variables set.

    Receive a workflow name in order to fetch the following information:
     - from couchdb: gets the output datasets and spec file
     - from phedex: gets dataset, block, files information
     - from dbs: gets dataset, block and files information
    """

    usage = "usage: %prog -w workflowName -f fileName -c cmswebUrl -r reqMgrUrl [-2|--reqmgr2] [-v|--verbose]"
    parser = OptionParser(usage=usage)
    parser.add_option('-w', '--workflow', help='A single workflow name.', dest='workflow')
    parser.add_option('-f', '--file', help='Plain text file containing request names (one per line).', dest='inputFile')
    parser.add_option('-c', '--cms', help='CMSWEB url to talk to DBS/PhEDEx. E.g: cmsweb.cern.ch', dest='cms')
    parser.add_option('-r', '--reqmgr', help='Request Manager URL. Example: couch-dev1.cern.ch', dest='reqmgr')
    parser.add_option('-2', '--reqmgr2', help='Set this variable for reqmgr2 requests.', dest='reqmgr2', action="store_true")
    parser.add_option('-v', '--verbose', help='Enable verbose mode.', dest='verbose', action="store_true")
    (options, args) = parser.parse_args()

    reqName = []
    if options.workflow:
        #reqName.append(options.workflow)
        reqName = options.workflow
    elif options.inputFile:
        with open(options.inputFile, 'r') as f:
            reqName.append(f.readlines())
    else:
        parser.error("You must provide either a workflow name or an input file name.")
        sys.exit(1)

    verbose = True if options.verbose else False
    cmsweb_url = "https://" + options.cms if options.cms else "https://cmsweb-testbed.cern.ch"
    reqmgr_url = "https://" + options.reqmgr if options.reqmgr else "https://cmsweb-testbed.cern.ch"
    reqmgr2 = True if options.reqmgr2 else False

    ### Retrieve ReqMgr information
    # TODO: this information may not be available for all type of requests
    print reqName
    reqmgr_out = reqmgr_info(reqName, reqmgr_url, reqmgr2)
    if reqmgr_out['RequestStatus'] not in ['completed', 'closed-out', 'announced']:
        print "We cannot validate wfs in this state: %s" % reqmgr_out['RequestStatus']
        sys.exit(0)
    try:
        couch_input = {'TotalEstimatedJobs': reqmgr_out['TotalEstimatedJobs'],
                       'TotalInputEvents': reqmgr_out['TotalInputEvents'],
                       'TotalInputLumis': reqmgr_out['TotalInputLumis'],
                       'TotalInputFiles': reqmgr_out['TotalInputFiles']}
    except KeyError:
        raise AttributeError("Total* parameter not found in reqmgr_workload_cache database")

    ### handle harvesting case
    couch_datasets = reqmgr_outputs(reqName, reqmgr_url)
    harvesting(reqmgr_out, couch_datasets)
    if reqmgr_out['RequestType'] == 'DQMHarvest':
        print "We cannot validate DQMHarvest workflows, look it yourself at: ",
        print "https://cmsweb-testbed.cern.ch/dqm/dev/data/browse/ROOT/"
        sys.exit(0)

    couch_input['Comments'] = reqmgr_out['Comments'] if 'Comments' in reqmgr_out else ''
    print " - Comments: %s" % couch_input['Comments']
    couch_input['InputDataset'] = reqmgr_out['InputDataset'] if 'InputDataset' in reqmgr_out else ''
    if 'Task1' in reqmgr_out and 'InputDataset' in reqmgr_out['Task1']:
        couch_input['InputDataset'] = reqmgr_out['Task1']['InputDataset']
    elif 'Step1' in reqmgr_out and 'InputDataset' in reqmgr_out['Step1']:
        couch_input['InputDataset'] = reqmgr_out['Step1']['InputDataset']

    if 'RequestNumEvents' in reqmgr_out:
        couch_input['RequestNumEvents'] = reqmgr_out['RequestNumEvents']
    elif 'Task1' in reqmgr_out and 'RequestNumEvents' in reqmgr_out['Task1']:
        couch_input['RequestNumEvents'] = reqmgr_out['Task1']['RequestNumEvents']
    else:
        couch_input['RequestNumEvents'] = ''

    ### Retrieve CouchDB information
    # Get workload summary,  output samples, num of files and events
    couch_out = couch_info(reqName, reqmgr_url)
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
                couch_summary[dset] = {'events': couch_out["workflow_summary"]["output"][dset]["events"],
                                       'dset_files': couch_out["workflow_summary"]["output"][dset]["nFiles"],
                                       'dset_size': couch_out["workflow_summary"]["output"][dset]["size"]}
            else:
                couch_num_files[dset], couch_num_events[dset], couch_dset_size[dset] = '', '', ''
                couch_summary[dset] = {'events': '', 'dset_files': '', 'dset_size': ''}

    ### Retrieve PhEDEx information
    # Get general phedex info, number of files and size of dataset
    phedex_out = phedex_info(couch_datasets, cmsweb_url)
    phedex_datasets = [i['name'].split('#')[0] for i in phedex_out["blockreplicas"]["phedex"]["block"]]
    # phedex_datasets = couch_datasets
    phedex_datasets = list(set(phedex_datasets))
    phedex_num_files = {}
    phedex_dset_size = {}
    phedex_block_se = {}
    phedex_summary = {}
    for item in phedex_out["blockreplicas"]["phedex"]["block"]:
        dset = item['name'].split('#')[0]
        phedex_block_se[item['name']] = item['replica'][0]['node']
        ##        phedex_block_se[item['name']] = item['replica'][0]['se']
        if dset not in phedex_summary:
            phedex_summary[dset] = [{'block': item['name'],
                                     'files': item['files'],
                                     'bytes': item['bytes'],
                                     'is_open': item['is_open'],
                                     'node': item['replica'][0]['node'],
                                     'se': item['replica'][0]['se'],
                                     'custodial': item['replica'][0]['custodial'],
                                     'complete': item['replica'][0]['complete'],
                                     'subscribed': item['replica'][0]['subscribed'],
                                     'dset_size': item['bytes'],
                                     'num_block': 1,
                                     'dset_files': item['files']}]
        else:
            phedex_summary[dset].append({'block': item['name'],
                                         'files': item['files'],
                                         'bytes': item['bytes'],
                                         'is_open': item['is_open'],
                                         'node': item['replica'][0]['node'],
                                         'se': item['replica'][0]['se'],
                                         'custodial': item['replica'][0]['custodial'],
                                         'complete': item['replica'][0]['complete'],
                                         'subscribed': item['replica'][0]['subscribed']})
            phedex_summary[dset][0]['dset_size'] += item['bytes']
            phedex_summary[dset][0]['num_block'] += 1
            phedex_summary[dset][0]['dset_files'] += item['files']
    for dset in phedex_datasets:
        if dset in phedex_summary:
            phedex_num_files[dset] = phedex_summary[dset][0]['dset_files']
            phedex_dset_size[dset] = phedex_summary[dset][0]['dset_size']
        else:
            phedex_num_files[dset] = ''
            phedex_dset_size[dset] = ''

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
        dbs_out = dbs_info(dset, cmsweb_url)
        for item in dbs_out[dset]['blockorigin']:
            dbs_block_se[item['block_name']] = item['origin_site_name']
            if dset not in dbs_summary:
                dbs_summary[dset] = [{'block_name': item['block_name'],
                                      'file_count': item['file_count'],
                                      'block_size': item['block_size'],
                                      'open_for_writing': item['open_for_writing'],
                                      'se': item['origin_site_name'],
                                      'dset_size': 0,
                                      'num_block': 0,
                                      'dset_files': 0,
                                      'num_event': 0,
                                      'num_lumi': 0}]
            else:
                dbs_summary[dset].append({'block_name': item['block_name'],
                                          'file_count': item['file_count'],
                                          'block_size': item['block_size'],
                                          'open_for_writing': item['open_for_writing'],
                                          'se': item['origin_site_name']})
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
    print '' + split_line
    print '|' + ' ' * 25 + '| CouchDB | PhEDEx | DBS  |'
    print separ_line

    # Perform checks among the 3 services: dset_name, dset_files and dset_size
    comp_res = list_cmp(couch_summary.keys(), phedex_summary.keys(), dbs_summary.keys())
    print '| Same dataset name       | {comp_res:7s} | {comp_res:6s} | {comp_res:4s} |'.format(comp_res=comp_res)
    comp_res = list_cmp(couch_num_files, phedex_num_files, dbs_num_files)
    print '| Same number of files    | {comp_res:7s} | {comp_res:6s} | {comp_res:4s} | '.format(comp_res=comp_res)
    comp_res = list_cmp(couch_dset_size, phedex_dset_size, dbs_dset_size)
    print '| Same dataset size       | {comp_res:7s} | {comp_res:6s} | {comp_res:4s} | '.format(comp_res=comp_res)

    # Perform check between Couch and DBS only: num_events
    comp_res = list_cmp(couch_num_events, dbs_num_events)
    print '| Same number of events   | %-7s | %-6s | %-4s |' % (comp_res, '--', comp_res)

    comp_res = list_cmp([couch_input['TotalInputLumis']], list(set(dbs_num_lumis.values())))
    print '| Same number of lumis    | %-7s | %-6s | %-4s |' % (comp_res, '--', comp_res)

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

    print '| All blocks closed?      | %-7s | %-6s | %-4s |' % ('--', phe_res, dbs_res)

    # Check whether files are registered in the same SE
    comp_res = list_cmp(phedex_block_se, dbs_block_se)
    print '| Blocks in the same PNN? | %-7s | %-6s | %-4s |' % ('--', comp_res, comp_res)
    print split_line

    ### Starts VERBOSE mode for the information retrieved so far
    if verbose:
        couch_verbose(couch_input, couch_summary, False)
        # dbs_verbose(dbs_out, dbs_summary, False)
        dbs_verbose(dbs_summary, False)
        phedex_verbose(phedex_out, phedex_summary, False)


if __name__ == "__main__":
    sys.exit(main())
