#!/usr/bin/env python
"""

Draw plot using ROOT

"""

import sys,json
from optparse import OptionParser
from collections import Counter

from ROOT import TCanvas, TPad, TFile, TPaveLabel, TPaveText, TH1F, THStack
from ROOT import gROOT,kRed,kBlue,kWhite,gStyle

gROOT.Reset()
# gROOT.SetBatch(True)
# statistics display
gStyle.SetOptStat(1111)

# seconds per day	
sdays = 86400

def parseData(json_file_name, metric, step):
    """
    Extracts the values to be plotted applying some special tweaks
    according to the metric name
    """
    # open json
    inputdata = json.load(open(json_file_name))

    # available properties
    metrics = [ m for m in inputdata[0][step].keys() ]

    values = []
    if metric == 'ReadMBPerSec':
        for job in inputdata:
            values.append(float(job[step]['Timing-tstoragefile-read-totalMegabytes']/
                                job[step]['TotalJobTime']))
    elif metric == 'CPUModels':
        modelsCore = [ (job[step]['CPUModels'], job[step]['totalCPUs']) for job in inputdata ]
        print 'Frequency of CPUModels x TotalCores:\n%r' % Counter(modelsCore)
        modelsCore = list(set(modelsCore))
        print 'Sorted and unique CPUModel x TotalCores:\n%s' % modelsCore
        for pair in modelsCore:
            values = []
            values = [ job[step]['AvgEventTime'] for job in inputdata if job[step]['CPUModels'] in pair[0] and
                       job[step]['totalCPUs'] == pair[1] ]
            drawPlot(json_file_name, metric, step, values, ''.join(pair[0].split()))
        return
    else:
        if metric not in metrics:
            print "Inexistent metric: %s. Please chose one of the following:\n%s" % (metric, metrics)
            return
        else:
            values = [ job[step][metric] for job in inputdata ]

    drawPlot(json_file_name, metric, step, values)


def drawPlot(jsonName, metric, step, values, model=''):
    """
    Values contain a list of filtered values to be plotted
    """
    # create canvas
    c1 = TCanvas( 'c1', 'Histogram Drawing Options', 1024, 768 )
    
    # c1.SetBottomMargin(0.15)
    # c1.SetRightMargin(0.01)
    # c1.SetGridx(True)
    # c1.SetGridy(True)

    # create histogram(s)
    nbins = int(sorted(values)[-1]*1.1)
    maxbin = nbins
    if nbins < 1000: nbins = 1000
    histogram = TH1F(metric, metric, nbins, 0, maxbin)
    
    # fill histogram(s)
    for number in values:
        histogram.Fill(number)

    # # set histogram options
    # histogram.SetOptStats(11111)
    # valid.SetTitle('Events with DBS-status VALID, total: ' + PositiveIntegerWithCommas(total_valid))
    # valid.SetStats(False)
    # valid.GetXaxis().CenterLabels()
    # valid.GetXaxis().LabelsOption('v')
    # valid.SetLineWidth(2)
    # valid.SetLineColor(kBlue)
    # valid.SetFillColor(kBlue)
    # valid.SetFillStyle(1000)
    # if nBins < 40:
    #     valid.SetBarWidth(0.8)
    #     valid.SetBarOffset(0.1)

    # draw
    histogram.Draw()
    #histogram2.Draw("SAME")

    raw_input('Press <ret> to end -> ')
    
    # safe as pdf and gif
    c1.SaveAs(jsonName.replace('.json','_') + metric + '_' + step + model + '.pdf')
#    c1.SaveAs(json_file_name.replace('.json','') + '_' + property + '.gif')
    return


def main():
    usage="%prog <options>\n\nPrepares plot from input JSON file\n"

    parser = OptionParser(usage=usage)
    parser.add_option("-d", "--data", dest="data", help="Input data in JSON format", metavar="<data>")
    parser.add_option("-m", "--metric", dest="metric", help="Metric to be plotted from JSON input file", metavar="<metric>")
    parser.add_option("-c", "--cmsrun", dest="cmsrun", default=1, 
                      help="Needed for jobs with 2 cmsRun steps (provide an integer only)", metavar="<cmsrun>")
   
    (opts, args) = parser.parse_args()
    if not (opts.data):
        parser.print_help()
        parser.error('Please specify input data in JSON format using -d or --data')
    if not (opts.metric):
        parser.print_help()
        parser.error('Please specify a metric to plot from JSON input file using -p or --metric')
        
    parseData(opts.data, opts.metric, 'cmsRun'+str(opts.cmsrun))
    
    sys.exit(0);

if __name__ == "__main__":
    main()
