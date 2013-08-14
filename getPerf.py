#!/usr/bin/env python -w
import sys,urllib,urllib2,re,time,os
import json
import optparse
import httplib
import datetime
import itertools
import matplotlib
matplotlib.use('agg')
import matplotlib.pyplot as pp
import numpy as np

metrics = ['PeakValueVsize', 'AvgEventTime', 'TotalJobTime', 'PeakValueRss']

def getTaskNames(workflow):
    """
    This method gets the TaskName for Task1.
    In order to get the TaskName for Task2, it has also to get the
    InputTask and the InputFromOutputModule in the Task2 dict, 
    otherwise we are not able to build the correct URL.
    FIXME: it's not able to get TaskNames for Task3+
    """
    conn  =  httplib.HTTPSConnection('cmsweb.cern.ch', cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
    r1=conn.request('GET','/reqmgr/view/showWorkload?requestName=' + workflow)
    r2=conn.getresponse()
    data = r2.read()
    conn.close()
    data = data.replace('<br/>','')
    list = data.split('\n')
    taskNames = []
    nbTasks = 1
    # Get the number of tasks and the TaskName1
    for raw in list:
        if 'request.schema.Task1.TaskName' in raw:
            taskNames.append(raw.split("'")[1])
        elif 'request.schema.TaskChain' in raw:
            nbTasks = int(raw.split('=')[1])
            print "\nNumber of tasks: %d" % nbTasks
    for nb in range(2, nbTasks+1):
        task = 'request.schema.Task'+str(nb)
        for raw in list:
            if task+'.InputTask' in raw:
                previousTask = raw.split("'")[1]
            elif task+'.InputFromOutputModule' in raw:
                outputModule = raw.split("'")[1]
            elif task+'.TaskName' in raw:
                Task = raw.split("'")[1]
        if nb == 2:
            taskNames.append(previousTask+'/'+previousTask+'Merge'+outputModule+'/'+Task)
        else:
            previous = taskNames[-1]
            taskNames.append(previous+'/'+previousTask+'Merge'+outputModule+'/'+Task)
    print taskNames
    return taskNames

def getWorkloadSummary(workflow):
    """
    It retrieves the whole workloadSummary
    """
    conn  =  httplib.HTTPSConnection('cmsweb.cern.ch', cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
    #r1=conn.request('GET','/reqmgr/reqMgr/request?requestName=' + workflow)
    r1=conn.request('GET','/couchdb/workloadsummary/' + workflow)
    r2=conn.getresponse()
    data = r2.read()
    s = json.loads(data)
    conn.close()
    #except:
    #   print "Cannot get request (getWorkloadSummary) " 
    #   sys.exit(1)
    print "\nJust got the workloadSummary"
    return s

def getPerformance(workload, workflow, task):
    """
    It receives the workloadSummary and then looks for the performance
    information according to the workflow and the task path built earlier.
    Notice tasks greater or equal than 2 run over the merged output...
    Note2: we are dropping values=0.0 for 'average', so it does not count
    when calculating the average.
    """
    semiPath = '/'+workflow+'/'+task
    finalValues = {}
    print "\ngetPerformance():\t%s" % semiPath
    for i in metrics:
        finalValues[i] = []
        for j in workload['performance'][semiPath]['cmsRun1'][i]['histogram']:
            if j['nEvents']:
                if j['average'] != 0.0: 
                    avg = [j['average']]
                    finalValues[i].append(avg * j['nEvents'])
        finalValues[i] = list(itertools.chain.from_iterable(finalValues[i]))
    print "FinalValues: ", finalValues
    return finalValues
#    print "PeakValueVsize: ", workload['performance'][semiPath]['cmsRun1']['PeakValueVsize']['histogram']
#    print "AvgEventTime: ", workload['performance'][semiPath]['cmsRun1']['AvgEventTime']['histogram']
#    print "TotalJobTime: ", workload['performance'][semiPath]['cmsRun1']['TotalJobTime']['histogram']
#    print "PeakValueRss: ", workload['performance'][semiPath]['cmsRun1']['PeakValueRss']['histogram']

def getWorstOffenders(workload, workflow, task):
    """
    It receives the workloadSummary and then looks for the worst offenders
    """
    semiPath = '/'+workflow+'/'+task
    finalWorst = {}
    print "\ngetWorstOffenders():\t%s" % semiPath
    for i in metrics:
        finalWorst[i] = []
        for j in workload['performance'][semiPath]['cmsRun1'][i]['worstOffenders']:
            if not np.isinf(float(j['value'])):
                finalWorst[i].append(float(j['value']))
            else:
                print "\n?????????? I FOUND AN INF VALUE FOR:"
                print "Workflow: %s\tTask: %s\tValue:%f" % (workflow, task, float(j['value']))
    print "FinalWorst: ", finalWorst
    return finalWorst

def getYArray(metric, maximum):
    """
    It returns an array that will be used to create the grid lines in the plots (Y axis only)
    Starting with the premisse that plots won't support more than 50 horizontal lines.
    """
    step = float(maximum/30)
    ypos = []
    if metric == 'AvgEventTime':
        if step <= 0.5:
            ypos = np.arange(0., maximum+1, .5)
        elif 0.5 < step <= 1.0:
            ypos = np.arange(0., maximum+1, 1.0)
        elif 1. < step <= 5.0:
            ypos = np.arange(0., maximum+5, 5.0)
        elif 5. < step <= 10.0:
            ypos = np.arange(0., maximum+10, 10.0)
        elif 10. < step <= 20.0:
            ypos = np.arange(0., maximum+20, 20.0)
        elif 20. < step <= 30.0:
            ypos = np.arange(0., maximum+30, 30.0)
        elif 30. < step <= 50.0:
            ypos = np.arange(0., maximum+50, 50.0)
        elif 50. < step <= 100.0:
            ypos = np.arange(0., maximum+100, 100.0)
        else:
            ypos = np.arange(0., maximum+step, int(step+1))
    elif metric == 'PeakValueVsize':
        if step <= 200:
            ypos = np.arange(0, maximum+200, 200)
        else:
            ypos = np.arange(0, maximum+step, int(step+1))
    elif metric == 'PeakValueRss':
        if step <= 200:
            ypos = np.arange(0, maximum+200, 200)
        else:
            ypos = np.arange(0, maximum+step, int(step+1))
    else: # TotalJobTime
        if step <= 500:
            ypos = np.arange(0, maximum+500, 500)
        elif 500 < step <= 2000:
            ypos = np.arange(0, maximum+2000, 2000)
        elif 2000 < step <= 5000:
            ypos = np.arange(0, maximum+5000, 5000)
        elif 5000 < step <= 10000:
            ypos = np.arange(0, maximum+10000, 10000)
        elif 10000 < step <= 20000:
            ypos = np.arange(0, maximum+20000, 20000)
        elif 20000 < step <= 50000:
            ypos = np.arange(0, maximum+50000, 50000)
        else:
            ypos = np.arange(0, maximum+step, int(step+1))
    return ypos

def getWorstValue(iniWorst, xvalues):
    """
    Return the real worst case in the workflow, taking into consideration
    the worstOffenders and the xvalues list
    """
    print "Is iniWorst (%f) > max(xvalues) (%r)\n" % (iniWorst, xvalues)
    if iniWorst > (max(xvalues)):
        return iniWorst
    else:
        return (max(xvalues))

def makePlots(perf, worst):
    """
    It receives a dict of a dict of a dict containing the workflowName, 
    taskName/pathToTask and the performance values according to the metrics.
    It receives a dictionary with the worst values for those 4 metrics.
    Finally it makes a plot. 
    """
    print "\nmakePlots()"
    print "\nIterating over perf dict..."
    metrics = ['PeakValueVsize','PeakValueRss','AvgEventTime','TotalJobTime']
    steps = ['GEN-SIM', 'DIGI', 'RECO', 'ALCA']
    # iterating over metrics
    for metric in metrics:
        print " ****** Metric: %s ******" % metric
        xnames = {steps[0] : [], steps[1] : [], steps[2] : [], steps[3] : []}
        xvalues = {steps[0] : [], steps[1] : [], steps[2] : [], steps[3] : []}
        yworst = {steps[0] : [], steps[1] : [], steps[2] : [], steps[3] : []}
        # iterating over workflows
        for wf,val1 in perf.iteritems():
            # iterating over tasks
            for task,val2 in val1.iteritems():
                # Reducing the size of the workflow name
                aux = wf.split('_')[:-3]
                aux = '_'.join(aux)
                aux = aux.split('_')[4:]
                shortWf = '_'.join(aux)
                shortWf = shortWf[1:]
                # Fix for when the worstOffenders list is empty, then set it to [0]
                if not worst[wf][task][metric]:
                    worst[wf][task][metric] = [0]
                # Fix for when xvalues (aka val2[metric]) is an empty list, then set it to [0]
                if not val2[metric]:
                    val2[metric] = [0]
                if steps[1] in task:
                    xnames[steps[1]].append(shortWf)
                    # it gets the mean of all values in the array
                    xvalues[steps[1]].append(np.mean(val2[metric]))
                    # Gets the worst of the worstOffenders only
                    yworst[steps[1]].append(getWorstValue(max(worst[wf][task][metric]), val2[metric]))
                elif steps[2] in task:
                    xnames[steps[2]].append(shortWf)
                    xvalues[steps[2]].append(np.mean(val2[metric]))
                    yworst[steps[2]].append(getWorstValue(max(worst[wf][task][metric]), val2[metric]))
                elif steps[3] in task:
                    xnames[steps[3]].append(shortWf)
                    xvalues[steps[3]].append(np.mean(val2[metric]))
                    yworst[steps[3]].append(getWorstValue(max(worst[wf][task][metric]), val2[metric]))
                else: # means GEN-SIM step
                    xnames[steps[0]].append(shortWf)
                    xvalues[steps[0]].append(np.mean(val2[metric]))
                    yworst[steps[0]].append(getWorstValue(max(worst[wf][task][metric]), val2[metric]))
        for step in steps:
            # if here is one workflow for this step, then it must be plotted
            if xnames[step]:
                print "\nCreating plots for %s step and %s metric\n" % (step, metric)
                print "xnames: ", xnames[step]
                print "xvalues: ", xvalues[step]
                print "yworst: ", yworst[step]
                width = len(yworst[step]) * 0.5
                # width needs to be at least 3.5
                if width < 3.5:
                    width = 3.5
                fig = pp.figure(figsize=(width, 10))
                #pp.title('CMSSW_X_Y_Z: '+step+' performance')
                pp.title(step+': '+metric)
                pos = np.arange(len(xnames[step]))  # bars start in these points and have width=0.5
                posticks = np.arange(len(xnames[step]))+0.25        # ticks are placed in the middle of the bars
                print "pos: ", pos
                pp.bar(pos, xvalues[step], ecolor='r', width=0.5)
                pp.plot(posticks, yworst[step], 'r.', markersize=9)
                pp.xticks(posticks, xnames[step], rotation=90)
                # tweaking y axis
                ypos = getYArray(metric, max(yworst[step]))
                pp.yticks(ypos)
                ### Tweaking the figure
                fig.subplots_adjust(left=0.18, bottom=0.4)           # Automatically adjust subplot parameters to give specified padding
                #pp.ylabel(metric)
                #pp.grid(True)
                pp.grid(True, which='major')
                filename = step+'_'+metric+'.png'
                fig.savefig('/afs/cern.ch/work/a/amaltaro/www/testPlots/pileup/'+filename)
            else:
                print "\nNothing to plot for: %s and %s\n" % (step, metric)

def main():
    """
    Based on a list of workflows, it does the following steps for each wf:
     * getTaskNames(): receives the workflow name and retrieves the tasknames
       (including the inputTask/outputModule) and returns a list.
     * getWorkloadSummary(): receives a workflow name and retrieves the workloadSummary
     * Then for every Task, it gets the Performance and WorstOffenders information. The
       methods return a dictionary containing the metric names as key and a list of the
       averages as value
     * Finally it makes 4 plots: 1 for Gen-Sim level step, one for DIGI, one for RECO
       and the last one for ALCA.
    """
    #list = ['anlevin_RVCMSSW_6_2_0QCD_Pt_600_800_130714_020114_1012','anlevin_RVCMSSW_6_2_0TTbarLepton_130714_015656_8806']
    args=sys.argv[1:]
    if len(args) != 1:
        print "usage: python getPerf.py <inputFile_containing_a_list_of_workflows>"
        sys.exit(0)
    inputFile=args[0]
    f = open(inputFile, 'r')
    list = []
    for line in f:
        list.append(line.rstrip('\n'))
    print "This is the list of workflows: ", list
    f.close
    count = 1
    finalPerf = {}
    finalWorst = {}
    for workflow in list:
        print "\n%d: %s" % (count,workflow)
        taskNames = getTaskNames(workflow)
        workload = getWorkloadSummary(workflow)
        singlePerf = {}
        singleWorst = {}
        # 'task' is the whole task chain here. There is no need to save all of this in the final dicts 
        for task in taskNames:
            shortTask = task.split('/')[-1]
            singlePerf[shortTask] = getPerformance(workload, workflow, task)
            singleWorst[shortTask] = getWorstOffenders(workload, workflow, task)
        finalPerf[workflow] = singlePerf
        finalWorst[workflow] = singleWorst
        count+=1
    print "\n***** Time to make the plots *****\n"
    print "\nfinalPerf: ", finalPerf
    print "\nfinalWorst: ", finalWorst
    makePlots(finalPerf, finalWorst)
    print "\nEND_OF_SCRIPT"
    sys.exit(0)

if __name__ == "__main__":
        main()
