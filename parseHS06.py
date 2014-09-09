#!/usr/bin/env python
"""
Parses json file containing several performance metrics and calculates
the performance normalized in HS06. Writes a new json with this information

In general it should be used to parse the ouput of parseLogCollect.py script

Since it uses numpy, you need to setup your env by:
export SCRAM_ARCH=slc5_amd64_gcc462
cd /build/relval/CMSSW_5_3_0/src/
cmsenv
"""

import os, sys, json
from optparse import OptionParser
from numpy import mean, std

def getHS06(model):
    """
    Receives the CPUModel and then returns the hepspec value based on
    a dictionary populated by hand and extracted from (per core):
    http://alimonitor.cern.ch/hepspec/
    """
    hs06Dict = {
                'AMD Opteron(TM) Processor 6238' : 4.92,
                'AMD Opteron(tm) Processor 6320' : 5.96,
                'Intel(R) Xeon(R) CPU E5520 @ 2.27GHz' : 5.76,
                'Intel(R) Xeon(R) CPU E5645 @ 2.40GHz' : 5.57,
                'Intel(R) Xeon(R) CPU E5620 @ 2.40GHz' : 6.88
               }
    if model in hs06Dict.keys():
        return hs06Dict[model]
    else:
        print "HS06 not found for : %s" % model
        return None

def main():
    """
    Provide a json file as input. It will write out another file
    with the values HSP06 normalized. 
    """
    usage = "Usage: %prog -i inputFile"
    parser = OptionParser(usage = usage)
    parser.add_option('-i', '--inputFile', help = 'Input file containing the logCollect tarball names', dest = 'input')
    (options, args) = parser.parse_args()
    if not options.input:
        parser.error('You must provide an input file')
        sys.exit(1)


    inputdata = json.load(open(options.input))

    # x-checking lengths
    print "CPUModels length: %d" % len(inputdata['CPUModels'])
    print "averageCoreSpeed length: %d" % len(inputdata['averageCoreSpeed'])
    print "totalCPUs length: %d" % len(inputdata['totalCPUs'])
    print "AvgEventTime length: %d" % len(inputdata['AvgEventTime'])

    newMetrics = {
                 'CPUModels' : [],
                 'averageCoreSpeed' : [],
                 'totalCPUs' : [],
                 'AvgEventTime' : [],
                 'HS06' : [],
                 'AvgEventTimeHS06' : []
                 }

    for i, _ in enumerate(inputdata['AvgEventTime']):
        hs06 = getHS06(inputdata['CPUModels'][i])
        if not hs06:
            continue
        newMetrics['HS06'].append(hs06)
        newMetrics['CPUModels'].append(inputdata['CPUModels'][i])
        newMetrics['averageCoreSpeed'].append(inputdata['averageCoreSpeed'][i])
        newMetrics['totalCPUs'].append(inputdata['totalCPUs'][i])
        newMetrics['AvgEventTime'].append(inputdata['AvgEventTime'][i])
        avgEvtHS06 = float(inputdata['AvgEventTime'][i]/hs06)
        newMetrics['AvgEventTimeHS06'].append(avgEvtHS06)
        
    summary = {'AvgEventTime': {}, 'AvgEventTimeHS06': {}}
    for m in ['AvgEventTime', 'AvgEventTimeHS06']:
        # Rounding in 3 digits to be nicely viewed
        summary[m]['avg'] = "%.3f" % mean(newMetrics[m]) 
        summary[m]['std'] = "%.3f" % std(newMetrics[m]) 
        summary[m]['max'] = "%.3f" % max(newMetrics[m]) 
        summary[m]['min'] = "%.3f" % min(newMetrics[m]) 
        print "%-17s: %s" % (m, summary[m])

    filename = 'perf_' + options.input
    print "Dumping new dict to: %s" % filename
    with open(filename, 'w') as outFile:
        json.dump(newMetrics, outFile)
        outFile.close()

    sys.exit(0)

if __name__ == "__main__":
        main()
