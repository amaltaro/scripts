#!/usr/bin/env python
###############################################################################
"""
Example script to inject LHE files in DBS3
For real usage various "string" defined here needs to
be changed and input file list has to be given with size,
number of events, and checksum

Run it like crab2:

source /afs/cern.ch/cms/LCG/LCG-2/UI/cms_ui_env.sh
cmsrel CMSSW_5_3_1 (or whatever recent release)
cd CMSSW_5_3_1
cmsenv
source /afs/cern.ch/cms/ccs/wm/scripts/Crab/crab.sh
voms-proxy-init -voms cms

./InsertFiles.py

Useful information (feedback from Stefano and Yuyi):
 - acquisition_era: it must be LHE (or CRAB) to bypass the name validation.
 - create_by: DBS3 will automatically fill this field
 - creation_date: DBS3 will automatically fill this field

"""
###############################################################################

import pprint
from dbs.apis.dbsClient import DbsApi
import uuid
import sys


def createEmptyBlock(ds_info, origin_site_name):
    acquisition_era_config = {'acquisition_era_name':'LHE', 'start_date':1954}
    processing_era_config = {
        'processing_version': 2,
        'description': 'test_LHE_injection'}
    primds_config = {
        #'primary_ds_type': 'mc',
        'primary_ds_type': 'test',
        'primary_ds_name': ds_info['primary_ds']}

    dataset = "/%s/%s/%s" % (ds_info['primary_ds'],ds_info['processed_ds'],ds_info['tier'])
    dataset_config = {
        'physics_group_name': ds_info['group'],
        'dataset_access_type': 'VALID',
        'data_tier_name': ds_info['tier'],
        'processed_ds_name':ds_info['processed_ds'],
        'dataset': dataset }

    block_name = "%s#%s" % (dataset, str(uuid.uuid4()))
    block_config = {'block_name': block_name, \
                        'origin_site_name': origin_site_name, \
                        'open_for_writing': 0}

    dataset_conf_list = [{'app_name': ds_info['application'],
                        'global_tag': 'dummytag',
                        'output_module_label': 'out',
                        'pset_hash': 'dummyhash',
                        'release_version': ds_info['app_version']}]

    blockDict = { \
         'files': [],
         'processing_era': processing_era_config,
         'primds': primds_config,
         'dataset': dataset_config,
         'dataset_conf_list' : dataset_conf_list,
         'acquisition_era': acquisition_era_config,
         'block': block_config,
         'file_parent_list':[],
         'file_conf_list':[],
    }
    return blockDict


def addFilesToBlock(blockDict, files):
    blockDict['files'] = files
    blockDict['block']['file_count'] = len(files)
    blockDict['block']['block_size'] = sum([int(file['file_size']) for file in files])
    return blockDict


#===============================================================
# MAIN STARTS HERE
#===============================================================

#pick a DBS3 instance
#instance = 'dev'
instance = 'int'
#instance = 'prod'


if instance=='dev':
    #host = 'dbs3-dev01.cern.ch'
    host = 'cmsweb-dev.cern.ch'

if instance=='int':
    host = 'cmsweb-testbed.cern.ch'

if instance=='prod':
    host = 'cmsweb.cern.ch'

globReadUrl = 'https://%s/dbs/%s/global/DBSReader' % (host, instance)
globWriteUrl = 'https://%s/dbs/%s/global/DBSWriter' % (host, instance)
phy3ReadUrl = 'https://%s/dbs/%s/phys03/DBSReader' % (host, instance)
phy3WriteUrl = 'https://%s/dbs/%s/phys03/DBSWriter' % (host, instance)

readApi   = DbsApi(url=globReadUrl)
writeApi  = DbsApi(url=globWriteUrl)
#readApi   = DbsApi(url=phy3ReadUrl)
#writeApi  = DbsApi(url=phy3WriteUrl)


# INFORMATION TO BE PUT IN DBS3

# almost free text here, but beware WMCore/Lexicon.py
dataset_info = {
    'primary_ds'    : 'QCD_HT-100To250_8TeV-madgraph',
    'processed_ds'  : 'LHE-testAlan_Attempt3-v2',
    'tier'          : 'LHE',
    'group'         : 'GEN',
    'campaign_name' : 'LHE',
    'application'   : 'Madgraph',
    'app_version'   : 'Mad_5_1_3_30',
}
origin_site_name = 'srm-eoscms.cern.ch'

# in following line: no / at beginning but / at the end
# corresponds to the directory in EOS after /store/lhe
directory_path='5475/'
assert(directory_path[0]  != '/')
assert(directory_path[-1] == '/')


# FROM NOW ON NO FREE NAMES


# PREPARE COMMON ADDITIONAL INFORMATION FOR FILES
common_lfn_prefix = '/store/lhe/'
common_file_type  = 'LHE'
common_dummy_lumi = [{'lumi_section_num': 1, 'run_num': 1}]


# GET INPUT FILES LIST
# NOTE THAT CURRENTLY FILENAME HAS TO END WITH .root

myFilesList=['QCD_HT-100To250_8TeV-madgraph_10001.lhe']
inputFiles=[]
for i in myFilesList:
    aFile ={'name':"%s" % i, \
                'event_count': 1000000, \
                'file_size': 85246505, \
                'check_sum': '2291210323', \
                #'adler32':'deadbeef'}
                'adler32':'NOTSET'}
    inputFiles.append(aFile)


# LOOP ON ALL FILES, CREATE BLOCKS AND INSERT THEM


files_in_block=0
max_files_in_block=500

for file in inputFiles:
    if files_in_block == 0:
        files=[]
        blockDict = createEmptyBlock(dataset_info, origin_site_name)
    
    fileDic={}
    lfn=common_lfn_prefix + directory_path + file['name']
    fileDic['file_type'] = common_file_type
    fileDic['logical_file_name'] = lfn
    for key in ['check_sum','adler32','file_size','event_count']:
        fileDic[key] = file [key]
    fileDic['file_lumi_list'] = common_dummy_lumi
    
    files.append(fileDic)
    files_in_block += 1
    print "file count %d" % files_in_block
    if files_in_block == max_files_in_block:
        blockDict = addFilesToBlock(blockDict, files)
        print "insert block in DBS3: %s" % writeApi.url
        print "ALAN: just before writing to DBS."
        pprint.pprint(blockDict)
        sys.exit(0)
        writeApi.insertBulkBlock(blockDict)
        files_in_block = 0
    
    # end loop on input Files

# any leftovers ?

if files_in_block:
    blockDict = addFilesToBlock(blockDict, files)
    print "insert block in DBS3: %s" % writeApi.url
    print "ALAN: leftovers just before writing to DBS."
    pprint.pprint(blockDict)
    sys.exit(0)
    writeApi.insertBulkBlock(blockDict)

