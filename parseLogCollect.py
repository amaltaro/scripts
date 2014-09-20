#!/usr/bin/env python -u
import os, sys, json
import subprocess
from pprint import pprint
from optparse import OptionParser
from xml.dom import minidom
from xml.parsers.expat import ExpatError
from datetime import datetime
from copy import deepcopy
# Awesome, there is numpy in CMSSW env
from numpy import mean, std

### TODO: I'm cleaning up the write metrics because it looks like it's completely unreliable
### TODO: read metrics are also not very reliable, but ... let's keep them a bit longer

def getHS06(model):
    """
    Receives the CPUModel and then returns the hepspec value based on
    a dictionary populated by hand and extracted from (per core):
    http://alimonitor.cern.ch/hepspec/

    as a backup, we could use (though we do not have info per core) 
    http://w3.hepix.org/benchmarks/doku.php?id=bench:results_sl6_x86_64_gcc_445
    """
    hs06Dict = {
                'AMD Opteron(TM) Processor 6238' : 4.92,
                'AMD Opteron(tm) Processor 6320' : 5.96,
                'Intel(R) Xeon(R) CPU E5520 @ 2.27GHz' : 5.76,
                'Intel(R) Xeon(R) CPU E5645 @ 2.40GHz' : 5.57,
                'Intel(R) Xeon(R) CPU E5620 @ 2.40GHz' : 6.88,
                'AMD Opteron(tm) Processor 6376' : 8.71,            # 64 cores. Model not available, so using "AMD Opteron 6378" value
                'Intel(R) Xeon(R) CPU E5430 @ 2.66GHz' : 6.99,
                'Quad-Core AMD Opteron(tm) Processor 2389' : 6.44,  # 8 cores. Model not available, so using "Quad-Core AMD Opteron(tm) Processor 2382" value
                'AMD Opteron(tm) Processor 6128 HE' : 5.17,         # Did not find an HE model, so using the 6128 only
                'AMD Opteron(tm) Processor 6134' : 5.17             # 32 cores. Model not available, so using the same value for "AMD Opteron(tm) Processor 6128"
               }
    if model in hs06Dict.keys():
        return hs06Dict[model]
    else:
        print "WARNING: HS06 value not found for : %s" % model
    sys.exit(1)

### TODO: Need to implement the HS06 part for this method
def buildStructOfArrays(logCollects, metrics, writeOut = None):
    """
    It will create a dict of arrays where the array index corresponds to the same job.
    This array will contain 2 main dicts, the first is for cmsRun1 and the other for cmsRun2
    This structure takes less memory since it does not write the key names "job" times.

    Example: each key will contain a list of values (jobs)
    [{'AvgEventTime': [40.41, 37.4, 46.8], 'TotalJobCPU': [7772.4, 7764.5, 8349.0], etc}, {cmsRun2 etc}]
    """
    dictRun, results = [{}, {}], [{}, {}]

    for i, _ in enumerate(dictRun):
        for m in metrics:
            dictRun[i][m] = []

    numLogCollects = 0
    for logCollect in logCollects:
        numLogCollects += 1
        print "%s: processing logCollect number: %d" % (datetime.now().time(), numLogCollects)
        # uncompress the big logCollect
        command = ["tar", "xvf", logCollect]
        p = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        out, err = p.communicate()
        logArchives = out.split()
        for logArchive in logArchives:
            #print logArchive
            # then uncompress each tarball inside the big logCollect
            subcommand = ["tar", "-x", "cmsRun?/FrameworkJobReport.xml", "-zvf", logArchive]
            q = subprocess.Popen(subcommand, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            out, err = q.communicate()
            cmsRuns = sorted(out.split())
            for i, step in enumerate(cmsRuns):
                try:
                    xmldoc = minidom.parse(step)
                except ExpatError:
                    print "Ops, that's a very BAD file %s" % step
                    continue
                items = ( (item.getAttribute('Name'),item.getAttribute('Value')) for item in xmldoc.getElementsByTagName('Metric') )
                matched = [item for item in items if item[0] in metrics ]
                xmldoc.unlink()
                for ele in matched:
                    if ele[0] != 'CPUModels':
                        dictRun[i][ele[0]].append(float(ele[1]))
                    else:
                        dictRun[i][ele[0]].append(str(ele[1]))

    # Debug
    #pprint(dictRun)

    print "%s: calculating metrics now ..." % (datetime.now().time())
    for j, step in enumerate(dictRun):
        if not step:
            continue
        for k, v in step.iteritems():
            if not v:
                continue
            elif k == 'CPUModels':
                results[j][k] = list(set(v))
                continue
            results[j][k] = {}
            # Rounding in 3 digits to be nicely viewed
            results[j][k]['avg'] = "%.3f" % mean(v)
            results[j][k]['std'] = "%.3f" % std(v)
            results[j][k]['min'] = "%.3f" % min(v)
            results[j][k]['max'] = "%.3f" % max(v)

    # Printing outside the upper for, so we can kind of order it...
    for i, step in enumerate(results):
        if not step:
            continue
        print "\nResults for cmsRun%s:" % str(i+1)
        for metric in metrics: 
            print "%-47s : %s" % (metric, step[metric])

    if writeOut:
        print ""   
        for i, step in enumerate(dictRun):
            if not step['AvgEventTime']:
                continue
            filename = 'cmsRun' + str(i+1) + '_' + writeOut
            print "Dumping whole cmsRun%d json into %s" % (i+1, filename)
            with open(filename, 'w') as outFile:
                json.dump(step, outFile)
                outFile.close()

    print "Mining completed at %s" % (datetime.now().time())
    return

def buildStructOfDicts(logCollects, metrics, writeOut = None):
    """
    It will create an array of dicts where each dictionary contains the full information for a specific job.
    Each dictionary may contains one or two keys (cmsRun steps).
    This structure takes much more memory, since we write the keyname in a job basis.

    Example: a list with several dicts/jobs.
    [{'cmsRun1': {'HS06': 10, 'totalCPUs': 20}, 'cmsRun2': {'HS06': 20, 'totalCPUs': 40}}, {'cmsRun1': {'HS06': 30, 'totalCPUs': 60}, {'cmsRun2': {etc}}]
    """
    listJobs, innerDict = [], {}

    for m in metrics:
        innerDict[m] = None

    numLogCollects = 0
    for logCollect in logCollects:
        numLogCollects += 1
        print "%s: processing logCollect number: %d" % (datetime.now().time(), numLogCollects)
        # uncompress the big logCollect
        command = ["tar", "xvf", logCollect]
        p = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        out, err = p.communicate()
        logArchives = out.split()
        for logArchive in logArchives:
            job = {}
            #print logArchive
            # then uncompress each tarball inside the big logCollect
            subcommand = ["tar", "-x", "cmsRun?/FrameworkJobReport.xml", "-zvf", logArchive]
            q = subprocess.Popen(subcommand, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            out, err = q.communicate()
            cmsRuns = sorted(out.split())
            for i, step in enumerate(cmsRuns):
                try:
                    xmldoc = minidom.parse(step)
                except ExpatError:
                    print "Ops, that's a very BAD file %s" % step
                    continue
                items = ( (item.getAttribute('Name'),item.getAttribute('Value')) for item in xmldoc.getElementsByTagName('Metric') )
                matched = [item for item in items if item[0] in metrics ]
                xmldoc.unlink()

                tmpDict = deepcopy(innerDict)
                for ele in matched:
                    if ele[0] != 'CPUModels':
                        tmpDict[ele[0]] = float(ele[1])
                    else:
                        tmpDict[ele[0]] = str(ele[1])
                # calculates HS06 values
                if not matched:
                    continue
                hs06 = getHS06(tmpDict['CPUModels'])
                tmpDict['HS06'] = hs06
                tmpDict['AvgEventTimeHS06'] = float(tmpDict['AvgEventTime']/hs06)
                # add the cmsRunX dict to the job dict
                run = 'cmsRun' + str(i+1)
                job[run] = tmpDict
            # add the full job dict to the general one
            if job:
                listJobs.append(job)

    # Debug
    #pprint(listJobs)

    print "%s: calculating metrics now ..." % (datetime.now().time())
    results = {}
    for i in ['cmsRun1', 'cmsRun2']: 
        results[i] = {}
        for m in metrics:
            results[i][m] = []
#    print results

    for job in listJobs:
        for k, v in job.iteritems():
            for m in metrics:
                # results[cmsRunX][metric] = value of the metric
                results[k][m].append(v[m])

    # Debug
#    pprint(results)

    summary = {}
    for i in ['cmsRun1', 'cmsRun2']:
        summary[i] = {}
        for m in metrics:
            if m == 'CPUModels':
                summary[i][m] = list(set(results[i][m]))
                continue
            # Rounding in 3 digits to be nicely viewed
            summary[i][m] = {}
            summary[i][m]['avg'] = "%.3f" % mean(results[i][m])
            summary[i][m]['std'] = "%.3f" % std(results[i][m])
            summary[i][m]['min'] = "%.3f" % min(results[i][m])
            summary[i][m]['max'] = "%.3f" % max(results[i][m])

    # Printing outside the upper for, so we can kind of order it...
    for run, info in summary.iteritems():
        print "\nResults for %s" % run
        for metric, value in info.iteritems():
            print "%-47s : %s" % (metric, value)

    if writeOut:
        print ""
        filename = 'fullDict_' +  writeOut
        print "Dumping whole json into %s" % filename
        with open(filename, 'w') as outFile:
            json.dump(listJobs, outFile)
            outFile.close()

    print "Mining completed at %s" % (datetime.now().time())
    return

def main():
    """
    Provide a logCollect tarball as input (in your local machine) or a text file
    with their name.

    export SCRAM_ARCH=slc5_amd64_gcc462
    cd /build/relval/CMSSW_5_3_0/src/
    cmsenv
    """
    usage = "Usage: %prog -t tarball -i inputFile [-o outputFile] [--long] [--array] [--dic]"
    parser = OptionParser(usage = usage)
    parser.add_option('-t', '--tarball', help = 'Tarball for the logCollect jobs', dest = 'tar')
    parser.add_option('-i', '--inputFile', help = 'Input file containing the logCollect tarball names', dest = 'input')
    parser.add_option('-o', '--outputFile', help = 'Output file containing info in json format', dest = 'output')
    parser.add_option('-l', '--long', action = "store_true", 
                      help = 'Use it to make a long summary (27 metrics in total)', dest = 'long')
    parser.add_option('-a', '--array', action = "store_true", help = 'Produces a structure of arrays', dest = 'array')
    parser.add_option('-d', '--dict', action = "store_true", help = 'Produces an array of dictionaries', dest = 'dict')
    (options, args) = parser.parse_args()
    if not options.tar and not options.input:
        parser.error('You must either provide a logCollect tarball or a file with their names')
        sys.exit(1)
    if not options.array and not options.dict:
        parser.error('You must choose which data structure you want to build')
        sys.exit(1)

    if options.long:
        metrics = ["Timing-file-read-maxMsecs","Timing-tstoragefile-read-maxMsecs",
                   "Timing-tstoragefile-readActual-maxMsecs","Timing-file-read-numOperations",
                   "Timing-tstoragefile-read-numOperations","Timing-tstoragefile-readActual-numOperations",
                   "Timing-file-read-totalMegabytes","Timing-tstoragefile-read-totalMegabytes",
                   "Timing-tstoragefile-readActual-totalMegabytes","Timing-file-read-totalMsecs",
                   "Timing-tstoragefile-read-totalMsecs","Timing-tstoragefile-readActual-totalMsecs",
                   "Timing-file-write-maxMsecs","Timing-tstoragefile-write-maxMsecs",
                   "Timing-tstoragefile-writeActual-maxMsecs","Timing-file-write-numOperations",
                   "Timing-tstoragefile-write-numOperations","Timing-tstoragefile-writeActual-numOperations",
                   "Timing-file-write-totalMegabytes","Timing-tstoragefile-write-totalMegabytes",
                   "Timing-tstoragefile-writeActual-totalMegabytes","Timing-file-write-totalMsecs",
                   "Timing-tstoragefile-write-totalMsecs","Timing-tstoragefile-writeActual-totalMsecs",
                   "AvgEventTime", "TotalJobTime","CPUModels","averageCoreSpeed","totalCPUs"]
    else:
        # In some cases "Timing-file-*" is empty, so let's use "Timing-tstoragefile-*
        metrics = ["Timing-tstoragefile-read-maxMsecs","Timing-tstoragefile-read-numOperations",
                   "Timing-tstoragefile-read-totalMegabytes","Timing-tstoragefile-read-totalMsecs",
                   "Timing-file-write-totalMegabytes","AvgEventTime","TotalJobTime","TotalJobCPU",
                   "CPUModels","averageCoreSpeed","totalCPUs","HS06","AvgEventTimeHS06"]
        #metrics = ["AvgEventTime","CPUModels","averageCoreSpeed"]

    if options.tar:
        logCollects = [options.tar]
    elif options.input:
        logCollects = []
        f = open(options.input, 'r')
        for tar in f:
            tar = tar.rstrip('\n')
            logCollects.append(tar)

    if options.array:
        buildStructOfArrays(logCollects, metrics, options.output)
    elif options.dict:
        buildStructOfDicts(logCollects, metrics, options.output)

    sys.exit(0)

if __name__ == "__main__":
        main()
