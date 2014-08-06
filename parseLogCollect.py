#!/usr/bin/env python -u
import os, sys
import subprocess
from optparse import OptionParser


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

    for file in tarballs:
        subcommand = ["tar", "-x", "cmsRun1/FrameworkJobReport.xml", "-zvf", file]
        print "Subcommand: %s" % subcommand

        p = subprocess.Popen(subcommand, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    sys.exit(0)

if __name__ == "__main__":
        main()
