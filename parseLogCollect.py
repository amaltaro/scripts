#!/usr/bin/env python -u
import os, sys, json
import subprocess
import pprint
from optparse import OptionParser
from xml.dom import minidom
from math import sqrt
from datetime import datetime

### TODO: try to optimize it
### TODO: I'm cleaning up the write metrics because it looks like it's completely unreliable
### TODO: read metrics are also not very reliable, but ... let's keep them a bit longer
  
def main():
    """
    Provide a logCollect tarball as input (in your local machine) or a text file
    with their name.
    """
    usage = "Usage: %prog -l logCollect -i inputFile [-o outputFile] [--short]"
    parser = OptionParser(usage = usage)
    parser.add_option('-l', '--logCollet', help = 'Tarball for the logCollect jobs', dest = 'logCol')
    parser.add_option('-i', '--inputFile', help = 'Input file containing the logCollect tarball names', dest = 'input')
    parser.add_option('-o', '--outputFile', help = 'Output file containing info in json format', dest = 'output')
    parser.add_option('-s', '--short', action = "store_true", 
                      help = 'Use it for short summary (8 metrics instead of 24)', dest = 'short')
    (options, args) = parser.parse_args()
    if not options.logCol and not options.input:
        parser.error('You must either provide a logCollect tarball or a file with their names')
        sys.exit(1)
    if options.short:
        metrics = ["Timing-file-read-maxMsecs","Timing-file-read-numOperations",
                   "Timing-file-read-totalMegabytes","Timing-file-read-totalMsecs",
                   "Timing-file-write-totalMegabytes","AvgEventTime", "TotalJobTime"]
    else:
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
                   "AvgEventTime", "TotalJobTime"]

    if options.logCol:
        logCollects = [options.logCol]
    elif options.input:
        logCollects = []
        f = open(options.input, 'r')
        for tar in f:
            tar = tar.rstrip('\n')
            logCollects.append(tar)

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
            # then uncompress each tarball inside the big logCollect
            subcommand = ["tar", "-x", "cmsRun?/FrameworkJobReport.xml", "-zvf", logArchive] 
            q = subprocess.Popen(subcommand, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            out, err = q.communicate()
            cmsRuns = sorted(out.split())
            for i, step in enumerate(cmsRuns):
                xmldoc = minidom.parse(step)
                items = ( (item.getAttribute('Name'),item.getAttribute('Value')) for item in xmldoc.getElementsByTagName('Metric') )
                matched = [item for item in items if item[0] in metrics ]
                for ele in matched:
                    dictRun[i][ele[0]].append(float(ele[1]))

    # Calculates average, standard deviation (old way, no numpy available) and
    # finds the maximum and minimum values
    print "%s: calculating metrics now ..." % (datetime.now().time())
    for j, step in enumerate(dictRun):
        if not step:
            continue
        for k, v in step.iteritems():
            if not v:
                continue
            results[j][k] = {}
            results[j][k]['avg'] = float(sum(v))/len(v)
            results[j][k]['std'] = 0
            results[j][k]['min'] = v[0]
            results[j][k]['max'] = v[0]
            for i in v:
                results[j][k]['std'] += (results[j][k]['avg'] - i) ** 2
                results[j][k]['max'] = i if (i > results[j][k]['max']) else results[j][k]['max']
                results[j][k]['min'] = i if (i < results[j][k]['min']) else results[j][k]['min']
            results[j][k]['std'] = "%.3f" % sqrt(float(results[j][k]['std']/len(v)))
    
            # Rounding in 3 digits to be nicely viewed
            results[j][k]['avg'] = "%.3f" % results[j][k]['avg']
            results[j][k]['max'] = "%.3f" % results[j][k]['max']
            results[j][k]['min'] = "%.3f" % results[j][k]['min']
 
    # Printing outside the upper for, so we can kind of order it...
    for i, step in enumerate(results):
        if not step:
            continue
        print "\nResults for cmsRun%s:" % str(i+1)
        for metric in metrics: 
            print "%-47s : %s" % (metric, step[metric])

    if options.output:
        print ""
        for i, step in enumerate(dictRun):
            if not step['TotalJobTime']:
                continue
            filename = 'cmsRun' + str(i+1) + '_' + options.output
            print "Dumping whole cmsRun%d json into %s" % (i+1, filename)
            with open(filename, 'w') as outFile:
                json.dump(step, outFile)
                outFile.close()

    sys.exit(0)

if __name__ == "__main__":
        main()
