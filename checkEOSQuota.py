#!/usr/bin/env python
"""
checkEOSQuota.py

This script will check the CMS EOS quotas and will send a mail
notification in case there is less than 10% space available.
"""

import os
import sys
import subprocess
from optparse import OptionParser

### GLOBAL SETTINGS
PATHS = ('/eos/cms/store/', '/eos/cms/store/relval/', '/eos/cms/store/unmerged/')
EOS_SETUP= '/afs/cern.ch/project/eos/installation/cms/etc/setup.sh'
WARNING = 90 # integer threshold for quota warning

def main(argv=None):
    """
    Uses the email address provided to send emails in case of quota problems.
      - email : Mail where notification should be sent (Default: USER).
      - help : Print this menu.

    Example: python checkEOSQuota.py --email alanmalta@gmail.com,alan.malta@cern.ch
    """
    usage = "usage: python %prog -e email_address"
    parser = OptionParser(usage=usage)
    parser.add_option('-e', '--email', help='Email address to send emails to', dest='email')
    (options, args) = parser.parse_args()
    if not options.email:
        parser.error("Please provide an email address.")
        sys.exit(1)
    email = options.email

    # Workaround to get the eos command
    com = "source " + EOS_SETUP
    subprocess.call(com, shell=True)
    with open(EOS_SETUP, "r") as f:
        content = f.readlines()
    for line in content:
        if "alias eos=" in line:
            eos = line.split('"')[1] 

    # query the EOS space
    ### FORMAT
    # group      used bytes logi bytes used files aval bytes aval logib aval files filled[%]  vol-status ino-status
    # zh         6.64 PB    3.30 PB    2.12 M-    9.20 PB    4.60 PB    1.00 G-    72.14      ok         ok 
    for pa in PATHS:
        problem = False
        com = [eos, "quota",  pa]
        p = subprocess.Popen(com, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        out, err = p.communicate()
        output = [line for line in out.split('\n') if line]
        #print "\nQuota status for: %s" % pa
        #print output[6] 
        #print output[7]
        out = output[7].split()
        filled, volStatus, inoStatus = int(float(out[13])), out[14], out[15]
        if volStatus != 'ok' or inoStatus != 'ok':
            #print " ... sending ERROR email for %s" % pa
            sendMailNotification(email, pa, 'ERROR')
        elif filled > int(WARNING):
            #print " ... sending WARNING email for %s" % pa
            sendMailNotification(email, pa, 'WARNING')

        # verify amount of files
        converter = {'k-': 10e2, 'M-': 10e5, 'G-': 10e8}
        usedFi, usedU, avalFi, avalU = float(out[5]), out[6], float(out[11]), out[12]
        usedFi *= converter[usedU]
        avalFi *= converter[avalU]
        if int(usedFi/avalFi * 100) > int(WARNING):
            #print " ... sending WARNING email for %s" % pa
            sendMailNotification(email, pa, 'WARNING')

def sendMailNotification(email, path, level):
    """
    Sends an email to the list of recipients provided as argument
    reporting the EOS path which is about to face problems.
    """
    msg = "EOS quota exceeded (or over %s%%) for %s." % (WARNING, path)
    msg += " Please investigate it ASAP."

    # echo "MESSAGE" | mail -s "SUBJECT" MAIL_ADD
    command = 'echo "%s" | ' % msg
    command += 'mail -s "%s: eos quota for %s"' % (level, path)
    command += ' %s' % email
    subprocess.call(command, shell=True)

if __name__ == '__main__':
    sys.exit(main())
