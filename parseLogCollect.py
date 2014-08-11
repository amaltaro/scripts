#!/usr/bin/env python -u
import os, sys, json
import subprocess
import pprint
from optparse import OptionParser
from xml.dom import minidom
from math import sqrt

def main():
    """
    Provide a logCollect tarball as input (in your local machine).
    """
    usage = "Usage: %prog -l logCollect"
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
        readMetrics = ["Timing-file-read-maxMsecs","Timing-file-read-numOperations",
                       "Timing-file-read-totalMegabytes","Timing-file-read-totalMsecs"]
        writeMetrics = ["Timing-file-write-maxMsecs","Timing-file-write-numOperations",
                        "Timing-file-write-totalMegabytes","Timing-file-write-totalMsecs"]
    else:
        readMetrics = ["Timing-file-read-maxMsecs","Timing-tstoragefile-read-maxMsecs",
                       "Timing-tstoragefile-readActual-maxMsecs","Timing-file-read-numOperations",
                       "Timing-tstoragefile-read-numOperations","Timing-tstoragefile-readActual-numOperations",
                       "Timing-file-read-totalMegabytes","Timing-tstoragefile-read-totalMegabytes",
                       "Timing-tstoragefile-readActual-totalMegabytes","Timing-file-read-totalMsecs",
                       "Timing-tstoragefile-read-totalMsecs","Timing-tstoragefile-readActual-totalMsecs"]
        writeMetrics = ["Timing-file-write-maxMsecs","Timing-tstoragefile-write-maxMsecs",
                        "Timing-tstoragefile-writeActual-maxMsecs","Timing-file-write-numOperations",
                        "Timing-tstoragefile-write-numOperations","Timing-tstoragefile-writeActual-numOperations",
                        "Timing-file-write-totalMegabytes","Timing-tstoragefile-write-totalMegabytes",
                        "Timing-tstoragefile-writeActual-totalMegabytes","Timing-file-write-totalMsecs",
                        "Timing-tstoragefile-write-totalMsecs","Timing-tstoragefile-writeActual-totalMsecs"]

    if options.logCol:
        logCollects = [options.logCol]
    elif options.input:
        logCollects = []
        f = open(options.input, 'r')
        for tar in f:
            tar = tar.rstrip('\n')
            logCollects.append(tar)

    total = {}
    numtarballs = 0
    for tarball in logCollects:
        command = ["tar", "xvf", tarball]
        p = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        out, err = p.communicate()
        tarballs = out.split()
        for file in tarballs:
            subcommand = ["tar", "-x", "cmsRun1/FrameworkJobReport.xml", "-zvf", file]

            # making this call quiet, so /dev/null
            subprocess.call(subcommand, stdout=open(os.devnull, 'wb'))
            xmldoc = minidom.parse("cmsRun1/FrameworkJobReport.xml")
            items = ( (item.getAttribute('Name'),item.getAttribute('Value')) for item in xmldoc.getElementsByTagName('Metric') )
            matched = [item for item in items if item[0] in readMetrics or item[0] in writeMetrics ]
            #matched = [item for item in items if item[0] in readMetrics ]
            for ele in matched:
                if not numtarballs:
                    total[ele[0]] = [float(ele[1])]
                else:
                    total[ele[0]].append(float(ele[1]))
            numtarballs += 1

    # Calculates average, standard deviation (old way, no numpy available) and
    # finds the maximum and minimum values
    results = {}
    for k, v in total.iteritems():
        results[k] = {}
        results[k]['avg'] = float(sum(v))/len(v)
        results[k]['std'] = 0
        results[k]['min'] = v[0]
        results[k]['max'] = v[0]
        for i in v:
            results[k]['std'] += (results[k]['avg'] - i) ** 2
            results[k]['max'] = i if (i > results[k]['max']) else results[k]['max']
            results[k]['min'] = i if (i < results[k]['min']) else results[k]['min']
        results[k]['std'] = "%.3f" % sqrt(float(results[k]['std']/len(v)))

        # Rounding in 3 digits to be nicely viewed
        results[k]['avg'] = "%.3f" % results[k]['avg']
        results[k]['max'] = "%.3f" % results[k]['max']
        results[k]['min'] = "%.3f" % results[k]['min']
 
    # Printing outside the upper for, so we can kind of order it...
    for metric in readMetrics: 
        print "%-47s : %s" % (metric, results[metric])
    print ""
    for metric in writeMetrics: 
        print "%-47s : %s" % (metric, results[metric])

    if options.output:
        with open(options.output, 'w') as outFile:
            json.dump(total, outFile)
            outFile.close()

    sys.exit(0)

if __name__ == "__main__":
        main()
