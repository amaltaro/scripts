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
        for raw in list:
            if 'request.schema.Task2.InputTask' in raw:
                previousTask = raw.split("'")[1]
            elif 'request.schema.Task2.InputFromOutputModule' in raw:
                outputModule = raw.split("'")[1]
            elif 'request.schema.Task2.TaskName' in raw:
                Task = raw.split("'")[1]
        taskNames.append(previousTask+'/'+previousTask+'Merge'+outputModule+'/'+Task)
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
            finalWorst[i].append(float(j['value']))
    print "FinalWorst: ", finalWorst
    return finalWorst

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
    steps = ['DIGI', 'RECO']
    # iterating over metrics
    for metric in metrics:
        print " ****** Metric: %s ******" % metric
        xnames = {'DIGI' : [], 'RECO' : []}
        xvalues = {'DIGI' : [], 'RECO' : []}
        yworst = {'DIGI' : [], 'RECO' : []}
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
                if 'DIGI' in task:
                    xnames['DIGI'].append(shortWf)
                    # it gets the mean of all values in the array
                    xvalues['DIGI'].append(np.mean(val2[metric]))
                    # Gets the worst of the worstOffenders only
                    yworst['DIGI'].append(max(worst[wf][task][metric]))
                elif 'RECO' in task:
                    xnames['RECO'].append(shortWf)
                    # it gets the mean of all values in the array
                    xvalues['RECO'].append(np.mean(val2[metric]))
                    # Gets the worst of the worstOffenders only
                    yworst['RECO'].append(max(worst[wf][task][metric]))
        for step in steps:
            # if here is one workflow for this step, then it must be plotted
            if xnames[step]:
                print "\nCreating plots for %s step and %s metric\n" % (step, metric)
                print "xnames: ", xnames[step]
                print "xvalues: ", xvalues[step]
                print "yworst: ", yworst[step]
                fig = pp.figure(figsize=(4, 6))
                #pp.title('CMSSW_X_Y_Z: '+step+' performance')
                pp.title(step+': '+metric)
                pos = np.arange(len(xnames[step]))+0.5        # the bar centers on the x axis based on the # of workflows
                pp.bar(pos, xvalues[step], ecolor='r', align='center')
                pp.plot(pos, yworst[step], 'r.', markersize=11)
                pp.xticks(pos, xnames[step], rotation=80)
                # tweaking y axis
                ypos = []
                if metric == 'AvgEventTime':
                    ypos = np.arange(0., max(yworst[step])+1, .5)
                elif metric == 'PeakValueVsize':
                    ypos = np.arange(0, max(yworst[step])+500, 200)
                elif metric == 'PeakValueRss':
                    ypos = np.arange(0, max(yworst[step])+500, 200)
                else: # TotalJobTime
                    ypos = np.arange(0, max(yworst[step])+500, 500)
                if len(ypos):
                    pp.yticks(ypos)
                ### Tweaking the figure
                fig.subplots_adjust(bottom=0.3)           # Automatically adjust subplot parameters to give specified padding
                #pp.ylabel(metric)
                #pp.grid(True)
                pp.grid(True, which='major')
                filename = step+'_'+metric+'.png'
                fig.savefig('/afs/cern.ch/work/a/amaltaro/www/testPlots/'+filename)
            else:
                print "Nothing to plot for: %s and %s\n" % (step, metric)

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
    list = ['anlevin_RVCMSSW_6_2_0QCD_Pt_600_800_130714_020114_1012','anlevin_RVCMSSW_6_2_0TTbarLepton_130714_015656_8806']
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
