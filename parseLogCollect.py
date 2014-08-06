#!/usr/bin/env python -u
import os, sys
import subprocess
from optparse import OptionParser
from xml.dom import minidom

def main():
    """
    Provide a logCollect tarball as input
    """
    usage = "Usage: %prog -l logCollect"
    parser = OptionParser(usage = usage)
    parser.add_option('-l', '--logCollet', help = 'Tarball for the logCollect jobs', dest = 'logCol')
    (options, args) = parser.parse_args()
    if not options.logCol:
        parser.error('You must provide a logCollect tarball')
        sys.exit(1)
    else:
        print "You provided: %s" % options.logCol

    command = ["tar", "xvf", options.logCol]
    p = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out, err = p.communicate()
    tarballs = out.split()

    readMetrics = ["Timing-file-read-maxMsecs","Timing-tstoragefile-read-maxMsecs","Timing-tstoragefile-readActual-maxMsecs"
                   "Timing-file-read-numOperations","Timing-tstoragefile-read-numOperations","Timing-tstoragefile-readActual-numOperations",
                   "Timing-file-read-totalMegabytes","Timing-tstoragefile-read-totalMegabytes","Timing-tstoragefile-readActual-totalMegabytes",
                   "Timing-file-read-totalMsecs","Timing-tstoragefile-read-totalMsecs","Timing-tstoragefile-readActual-totalMsecs"]

    writeMetrics = ["Timing-file-write-maxMsecs","Timing-tstoragefile-write-maxMsecs","Timing-tstoragefile-writeActual-maxMsecs"
                   "Timing-file-write-numOperations","Timing-tstoragefile-write-numOperations","Timing-tstoragefile-writeActual-numOperations",
                   "Timing-file-write-totalMegabytes","Timing-tstoragefile-write-totalMegabytes","Timing-tstoragefile-writeActual-totalMegabytes",
                   "Timing-file-write-totalMsecs","Timing-tstoragefile-write-totalMsecs","Timing-tstoragefile-writeActual-totalMsecs"]

    for file in tarballs:
        subcommand = ["tar", "-x", "cmsRun1/FrameworkJobReport.xml", "-zvf", file]
        print "Subcommand: %s" % subcommand

        subprocess.call(subcommand)
        xmldoc = minidom.parse("cmsRun1/FrameworkJobReport.xml")
        items = ( (item.getAttribute('Name'),item.getAttribute('Value')) for item in xmldoc.getElementsByTagName('Metric') )
        #matching = [item for item in items if item[0] in readMetrics or item[0] in writeMetrics ]
        matching = [item for item in items if item[0] in readMetrics ]
        print matching

    sys.exit(0)

if __name__ == "__main__":
        main()
