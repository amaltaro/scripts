
import time
from os import mkdir, rmdir
from subprocess import call, Popen, PIPE

tcall, tmkdir, tpopen = 0, 0, 0

class Timer(object):
    def __init__(self, verbose=False):
        self.verbose = verbose

    def __enter__(self):
        self.start = time.time()
        return self

    def __exit__(self, *args):
        self.end = time.time()
        self.secs = self.end - self.start
        self.msecs = self.secs * 1000  # millisecs
        if self.verbose:
            print 'elapsed time: %f ms' % self.msecs


def runit():
    global tcall
    global tmkdir
    global tpopen
    with Timer() as t:
        listDir = ["job%d" % i for i in xrange(1000, 2000)]
        listDir.insert(0, 'mkdir')
        retcode = call(listDir)
    tcall += float(t.msecs)
   
    with Timer() as t:
        listDir = ["job%d" % i for i in xrange(2000, 3000)]
        for j in listDir:
            mkdir(j)
    tmkdir += float(t.msecs)

    with Timer() as t:
        listDir = ["job%d" % i for i in xrange(3000, 4000)]
        listDir.insert(0, 'mkdir')
        pipe = Popen(listDir, stdout = PIPE, stderr = PIPE, shell = False)
    tpopen += float(t.msecs)

def cleanit():
    listDir = ["job%d" % i for i in xrange(1000, 4000)]
    for j in listDir:
        rmdir(j)

for i in range(0, 100):
    runit()
    cleanit()

print "=> call function elasped time (ms): ",
print float(tcall)/100.0
print "=> mkdir function elasped time (ms): ",
print float(tmkdir)/100.0
print "=> Popen function elasped time (ms): ",
print float(tpopen)/100.0
