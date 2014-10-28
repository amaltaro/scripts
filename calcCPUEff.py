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


def buildStructOfDicts(logCollects, metrics, writeOut = None, cores = 1):
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

    print "CPU eff will be calculated for %d cores" % cores
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
                    tmpDict[ele[0]] = float(ele[1])
                # calculates job cpu efficiency
                tmpDict['JobCPUEff'] = tmpDict['TotalJobCPU']/(tmpDict['TotalJobTime']*cores)

                if not matched:
                    continue
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
    #print results

    for job in listJobs:
        for k, v in job.iteritems():
            for m in metrics:
                # results[cmsRunX][metric] = value of the metric
                results[k][m].append(v[m])

    # Debug
    #pprint(results)

    summary = {}
    for i in ['cmsRun1', 'cmsRun2']:
        summary[i] = {}
        for m in metrics:
            if m == 'CPUModels':
                summary[i][m] = list(set(results[i][m]))
                continue
            # Rounding in 3 digits to be nicely viewed
            if results[i][m]:
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
    usage = "Usage: %prog -t tarball -i inputFile [-o outputFile]"
    parser = OptionParser(usage = usage)
    parser.add_option('-t', '--tarball', help = 'Tarball for the logCollect jobs', dest = 'tar')
    parser.add_option('-i', '--inputFile', help = 'Input file containing the logCollect tarball names', dest = 'input')
    parser.add_option('-o', '--outputFile', help = 'Output file containing info in json format', dest = 'output')
    parser.add_option('-c', '--cores', help = 'Number of cores per job', dest = 'cores')
    (options, args) = parser.parse_args()
    if not options.tar and not options.input:
        parser.error('You must either provide a logCollect tarball or a file with their names')
        sys.exit(1)
    cores = int(options.cores) if options.cores else 1

    metrics = ["TotalJobTime","TotalJobCPU","JobCPUEff"]

    if options.tar:
        logCollects = [options.tar]
    elif options.input:
        logCollects = []
        f = open(options.input, 'r')
        for tar in f:
            tar = tar.rstrip('\n')
            logCollects.append(tar)

    buildStructOfDicts(logCollects, metrics, options.output, cores)

    sys.exit(0)

if __name__ == "__main__":
        main()
