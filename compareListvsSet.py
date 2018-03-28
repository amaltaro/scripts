from __future__ import print_function
import timeit

NUM_ITER = 100000

def testList():
    l = []
    total = 0
    for i in range(NUM_ITER):
        if i in l:
            continue
        l.append(i)
    for i in range(10000, 20000):
        if i in l:
            continue
        l.append(i)
    for i in l:
        total += i

def testSet():
    s = set()
    total = 0
    for i in range(NUM_ITER):
        s.add(i)
    for i in range(10000, 20000):
        s.add(i)
    for i in s:
        total += i

if __name__ == '__main__':
    print("Test with %d iterations." % NUM_ITER)
    listT = timeit.timeit("testList()", setup="from __main__ import testList", number=3)
    setT = timeit.timeit("testSet()", setup="from __main__ import testSet", number=3)
    print("Time spent with a list object: %f" % listT)
    print("Time spent with a set object: %f" % setT)
