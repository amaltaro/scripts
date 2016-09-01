#!/usr/bin/env python -u
import sys
import json
import tarfile
from xml.dom import minidom
from xml.parsers.expat import ExpatError

"""
Input merged file:  /store/mc/RunIISummer15GS/GluGluToHiggs0Mf05ph0ContinToZZTo4tau_M125_10GaSM_13TeV_MCFM701_pythia8/GEN-SIM/MCRUN2_71_V1-v1/60000/9ACBF741-2058-E611-B490-02163E015D16.root

logCollect tarball: srm://srm-cms.cern.ch:8443/srm/managerv2?SFN=/castor/cern.ch/cms/store/logs/prod/2016/08/WMAgent/pdmvserv_HIG-RunIISummer15GS-01627_00417_v0__160721_121444_9806/pdmvserv_HIG-RunIISummer15GS-01627_00417_v0__160721_121444_9806-LogCollect-b6613160f4-1-logs.tar

Downloading the tarball from CERN EOS (lxplus):
xrdcp root://eoscms.cern.ch//eos/cms//store/logs/prod/2016/08/WMAgent/pdmvserv_HIG-RunIISummer15GS-01627_00417_v0__160721_121444_9806/pdmvserv_HIG-RunIISummer15GS-01627_00417_v0__160721_121444_9806-LogCollect-b6613160f4-1-logs.tar .

Untar a single file:
tar xvf pdmvserv_HIG-RunIISummer15GS-01627_00417_v0__160721_121444_9806-LogCollect-b6613160f4-1-logs.tar WMTaskSpace/logCollect1/d5f287fa-57e3-11e6-a591-001e67abf094-172-0-logArchive.tar.gz

Then untar the FJR from the new tarball:
tar xvzf WMTaskSpace/logCollect1/d5f287fa-57e3-11e6-a591-001e67abf094-172-0-logArchive.tar.gz cmsRun1/FrameworkJobReport.xml

Example:
{'logCollect': ['srm://srm-cms.cern.ch:8443/srm/managerv2?SFN=/castor/cern.ch/cms/store/logs/prod/2016/08/WMAgent/pdmvserv_HIG-RunIISummer15GS-01627_00417_v0__160721_121444_9806/pdmvserv_HIG-RunIISummer15GS-01627_00417_v0__160721_121444_9806-LogCollect-b6613160f4-1-logs.tar'],
 'queries': [['/store/unmerged/RunIISummer15GS/GluGluToHiggs0Mf05ph0ContinToZZTo4tau_M125_10GaSM_13TeV_MCFM701_pythia8/GEN-SIM/MCRUN2_71_V1-v1/60000/CE884A69-FB57-E611-B4E4-FA163EB50311.root',
              '/store/unmerged/RunIISummer15GS/GluGluToHiggs0Mf05ph0ContinToZZTo4tau_M125_10GaSM_13TeV_MCFM701_pythia8/GEN-SIM/MCRUN2_71_V1-v1/60000/EA23211F-F457-E611-9A6F-02163E014C8F.root',
              '/store/unmerged/RunIISummer15GS/GluGluToHiggs0Mf05ph0ContinToZZTo4tau_M125_10GaSM_13TeV_MCFM701_pythia8/GEN-SIM/MCRUN2_71_V1-v1/60000/80B92337-F457-E611-8988-FA163EC6A7CA.root',
              '/store/unmerged/RunIISummer15GS/GluGluToHiggs0Mf05ph0ContinToZZTo4tau_M125_10GaSM_13TeV_MCFM701_pythia8/GEN-SIM/MCRUN2_71_V1-v1/60000/BE9CEC8D-F457-E611-993A-02163E01643D.root',
              '/store/unmerged/RunIISummer15GS/GluGluToHiggs0Mf05ph0ContinToZZTo4tau_M125_10GaSM_13TeV_MCFM701_pythia8/GEN-SIM/MCRUN2_71_V1-v1/60000/323389AA-F457-E611-9004-FA163E1A547C.root',
              '/store/unmerged/RunIISummer15GS/GluGluToHiggs0Mf05ph0ContinToZZTo4tau_M125_10GaSM_13TeV_MCFM701_pythia8/GEN-SIM/MCRUN2_71_V1-v1/60000/86DE0D6C-F557-E611-91A6-FA163E589D8E.root',
              '/store/unmerged/RunIISummer15GS/GluGluToHiggs0Mf05ph0ContinToZZTo4tau_M125_10GaSM_13TeV_MCFM701_pythia8/GEN-SIM/MCRUN2_71_V1-v1/60000/6AA37673-F557-E611-B7CD-FA163EB612D4.root',
              '/store/unmerged/RunIISummer15GS/GluGluToHiggs0Mf05ph0ContinToZZTo4tau_M125_10GaSM_13TeV_MCFM701_pythia8/GEN-SIM/MCRUN2_71_V1-v1/60000/AA21797B-F557-E611-844F-FA163ECBFBF6.root',
              '/store/unmerged/RunIISummer15GS/GluGluToHiggs0Mf05ph0ContinToZZTo4tau_M125_10GaSM_13TeV_MCFM701_pythia8/GEN-SIM/MCRUN2_71_V1-v1/60000/20E7EA77-F657-E611-B970-FA163E1EC457.root',
              '/store/unmerged/RunIISummer15GS/GluGluToHiggs0Mf05ph0ContinToZZTo4tau_M125_10GaSM_13TeV_MCFM701_pythia8/GEN-SIM/MCRUN2_71_V1-v1/60000/66AC4615-F757-E611-BDE3-FA163E48182A.root',
              '/store/unmerged/RunIISummer15GS/GluGluToHiggs0Mf05ph0ContinToZZTo4tau_M125_10GaSM_13TeV_MCFM701_pythia8/GEN-SIM/MCRUN2_71_V1-v1/60000/027EB1AA-F757-E611-94B5-FA163E5C0B9A.root'],
             ['/store/unmerged/logs/prod/2016/8/1/pdmvserv_HIG-RunIISummer15GS-01627_00417_v0__160721_121444_9806/MonteCarloFromGEN/0000/0/d5f287fa-57e3-11e6-a591-001e67abf094-172-0-logArchive.tar.gz',
              '/store/unmerged/logs/prod/2016/8/1/pdmvserv_HIG-RunIISummer15GS-01627_00417_v0__160721_121444_9806/MonteCarloFromGEN/0000/0/d5f287fa-57e3-11e6-a591-001e67abf094-188-0-logArchive.tar.gz',
              '/store/unmerged/logs/prod/2016/8/1/pdmvserv_HIG-RunIISummer15GS-01627_00417_v0__160721_121444_9806/MonteCarloFromGEN/0000/0/d5f287fa-57e3-11e6-a591-001e67abf094-217-0-logArchive.tar.gz',
              '/store/unmerged/logs/prod/2016/8/1/pdmvserv_HIG-RunIISummer15GS-01627_00417_v0__160721_121444_9806/MonteCarloFromGEN/0000/0/d5f287fa-57e3-11e6-a591-001e67abf094-282-0-logArchive.tar.gz',
              '/store/unmerged/logs/prod/2016/8/1/pdmvserv_HIG-RunIISummer15GS-01627_00417_v0__160721_121444_9806/MonteCarloFromGEN/0000/0/d5f287fa-57e3-11e6-a591-001e67abf094-288-0-logArchive.tar.gz',
              '/store/unmerged/logs/prod/2016/8/1/pdmvserv_HIG-RunIISummer15GS-01627_00417_v0__160721_121444_9806/MonteCarloFromGEN/0000/0/d5f287fa-57e3-11e6-a591-001e67abf094-345-0-logArchive.tar.gz',
              '/store/unmerged/logs/prod/2016/8/1/pdmvserv_HIG-RunIISummer15GS-01627_00417_v0__160721_121444_9806/MonteCarloFromGEN/0000/0/d5f287fa-57e3-11e6-a591-001e67abf094-425-0-logArchive.tar.gz',
              '/store/unmerged/logs/prod/2016/8/1/pdmvserv_HIG-RunIISummer15GS-01627_00417_v0__160721_121444_9806/MonteCarloFromGEN/0000/0/d5f287fa-57e3-11e6-a591-001e67abf094-155-0-logArchive.tar.gz',
              '/store/unmerged/logs/prod/2016/8/1/pdmvserv_HIG-RunIISummer15GS-01627_00417_v0__160721_121444_9806/MonteCarloFromGEN/0000/0/d5f287fa-57e3-11e6-a591-001e67abf094-227-0-logArchive.tar.gz',
              '/store/unmerged/logs/prod/2016/8/1/pdmvserv_HIG-RunIISummer15GS-01627_00417_v0__160721_121444_9806/MonteCarloFromGEN/0000/0/d5f287fa-57e3-11e6-a591-001e67abf094-281-0-logArchive.tar.gz',
              '/store/unmerged/logs/prod/2016/8/1/pdmvserv_HIG-RunIISummer15GS-01627_00417_v0__160721_121444_9806/MonteCarloFromGEN/0000/0/d5f287fa-57e3-11e6-a591-001e67abf094-404-0-logArchive.tar.gz']]}
"""

def getText(nodelist):
    """
    Util to get the text. Source from:
    https://docs.python.org/2/library/xml.dom.minidom.html
    """
    rc = []
    for node in nodelist:
        if node.nodeType == node.TEXT_NODE:
            rc.append(node.data)
    return ''.join(rc)

def extractFile(logArchName):
    """
    Extract the file and returns the xml data
    """
    rawdata = None
    logArchName = 'WMTaskSpace/logCollect1/' + logArchName
    fjrFile = 'cmsRun1/FrameworkJobReport.xml'
    try:
        with tarfile.open(logArchName, 'r:gz') as tf:
            fjr = tf.extractfile(fjrFile)
            rawdata = fjr.read()
    except IOError:
        print("File %s not found in the logCollect tarball" % logArchName)
    except KeyError:
        print("Failed to find %s in the tarballs dir." % fjrFile)
    except Exception:
        print("Something really bad happened while extracting FJR from the logArchive tarball")
    return rawdata

def getInputFileInfo(dom):
    """
    Retrieves information about the input files
    """
    listLumis = []
    #print("Input:")
    for item in dom:
        lfn = item.getElementsByTagName("LFN")[0]
        #print("  LFN    : %s" % getText(lfn.childNodes))
        events = item.getElementsByTagName("EventsRead")[0]
        #print("  Events : %s" % getText(events.childNodes))
        runs = item.getElementsByTagName("Runs")
        for run in runs:
            r = run.getElementsByTagName("Run")
            #print("  Runs   : %s" % r[0].attributes['ID'].value)
            lumis = r[0].getElementsByTagName("LumiSection")
            for lumi in lumis:
                listLumis.append(int(lumi.attributes['ID'].value))
            #print("  Lumis  : %s" % listLumis)
    return listLumis


def getOutputFileInfo(dom):
    """
    Retrieves info about the output data
    """
    #print("Output:")
    for item in dom:
        lfn = item.getElementsByTagName("LFN")[0]
        pfn = item.getElementsByTagName("PFN")[0]
        guid = item.getElementsByTagName("GUID")[0]
        lfn = getText(lfn.childNodes)
        pfn = getText(pfn.childNodes)
        guid = getText(guid.childNodes)
        lfn = lfn.replace(pfn, guid) + '.root'
        #print("  LFN    : %s" % lfn)

def doTheWork(logArchName):
    """
    Do all the hard work
    """
    rawdata = extractFile(logArchName)
    if not rawdata:
        print("No data retrieved from the extracted file")
        return []

    try:
        xmldoc = minidom.parseString(rawdata)
    except ExpatError:
        print "I cannot parse the damn xml file"
        return []

    lumis = getInputFileInfo(xmldoc.getElementsByTagName('InputFile'))
    getOutputFileInfo(xmldoc.getElementsByTagName('File'))
    xmldoc.unlink()
    return lumis

def main():
    """
    This script assumes you have done the following steps already:
     1. downloaded the logCollect tarball to the current directory
     2. added the list of files/logArchive to the global vars on the top
     3. untarred the logcollect tarball in the current dir
    """
    if len(sys.argv) != 2:
        print("Usage: python parseLogCollect.py input_json_file")
        sys.exit(1)
    inputFile = sys.argv[1]
    with open(inputFile) as jo:
        wmarchivedata = json.load(jo)

    totalLumis = []
    for logArch in wmarchivedata['queries'][1]:
        logArch = logArch.split('/')[-1]
        totalLumis.extend(doTheWork(logArch))

    print("\nFinal lumis: %s" % totalLumis)
    sys.exit(0)

if __name__ == "__main__":
        main()
