#!/usr/bin/env python -u

import pickle

def main():
    """
    It will unpickle the PSet under cmsRun1/PSet.pkl in
    your current directory.
    Note you need first to untar the logarchive tarball.

    The output file will be in the current dir name as
    myPSet.py
    """
    pickleHandle = open('cmsRun1/PSet.pkl','rb')
    process = pickle.load(pickleHandle)

    f = open('myPSet.py', 'w')
    f.write(process.dumpPython())
    f.close()

if __name__ == "__main__":
        main()
