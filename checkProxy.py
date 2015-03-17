#!/usr/bin/env python
"""
checkProxy.py

This script will check is the proxy is still valid. In case the proxy
has less than three hours of life a mail notification is sent.

Blame Diego if this doesn't work: direyes@cern.ch

"""

import os
import sys
import subprocess
import time
import datetime
import getopt
import re
import tempfile

def main(argv):
    """
    This is an utility that checks the proxy validity and sends an email in
    case the time left is lesser than --time.
    Arguments:
    --proxy     : Proxy file location (Default: X509_USER_PROXY)
    --myproxy   : Check the log term proxy in myproxy (Default: True)
    --mail      : Mail where notification should be sent (Default: USER).
    --send-mail : <True|False> Send mail notification (Default: True).
    --time      : Minimun time left [hours]. It should be an integer. (Default: 48 h).
    --retrieve  : Retrieve a short proxy from myproxy and add voms extension to it (Default: False)
    --verbose   : Print output messages.
    --help      : Print this menu.

    Example: python /data/amaltaro/checkProxy.py --proxy /data/certs/myproxy.pem
    --time 72 --send-mail True --mail alanmalta@gmail.com,alan.malta@cern.ch --verbose
    """

    valid = ["proxy=", "myproxy=", "mail=", "send-mail=", "time=", "retrieve=", "verbose", "help"]
    ### // Default values
    proxy = os.getenv('X509_USER_PROXY')
    myproxy = False
    verbose = False
    mail = os.getenv('USER')
    sendMail = True
    retrieve = False
    time = 3
    host = os.getenv('HOSTNAME')

    try:
        opts, args = getopt.getopt(argv, "", valid)
    except getopt.GetoptError, ex:
        print main.__doc__
        print ''
        print str(ex)
        print ''
        sys.exit(1)
    
    ### // Handle arguments given in the command line
    for opt, arg in opts:
        if opt == "--proxy":
            proxy = arg
            if proxy.startswith("~/"):
                proxy = os.getenv['HOME'] + proxy[1:]
            if not os.path.exists(proxy) :
                print "Proxy File does not exist,"
                sys.exit(2)
        if opt == "--mail":
            mail = arg
        if opt == "--myproxy":
            myproxy = arg
        if opt == "--send-mail":
            sendMail = arg
        if opt == "--retrieve":
            retrieve = arg
        if opt == "--time":
            time = int(arg)
            if time < 1:
                print main.__doc__
                print "Invalid time"
                raise sys.exit(3)
        if opt == "--verbose" :
            verbose = True
        if opt == "--help":
            print main.__doc__
            sys.exit(0)

    ### // Retrieves short term proxy info
    command = ["voms-proxy-info", "-all", "-file", str(proxy)]
    p = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out, err = p.communicate()
    proxyInfo = [line for line in out.split('\n') if line]
    timeLeft = processTimeLeft(host, sendMail, verbose, proxyInfo, time, mail)

    if myproxy:
        os.environ["X509_USER_CERT"] = proxy
        os.environ["X509_USER_KEY"] = proxy
#        print "%s" % os.getenv("X509_USER_CERT")
        command = ["myproxy-info", "-v", "-l", "amaltaro", "-s", "myproxy.cern.ch", "-k", "amaltaroCERN"]
        p = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        out, err = p.communicate()
        proxyInfo = [line for line in out.split('\n') if line]
        timeLeft = processTimeLeft(host, sendMail, verbose, proxyInfo, time, mail)


def sendMailNotification(mail, message, proxyInfo='', verbose=False):
    host = os.getenv('HOSTNAME')
    os.chdir(os.environ['HOME'])
    if verbose:
        print "Host:", host
        print "Home path:", os.environ['HOME']
    #append proxy info to message
    for line in proxyInfo:
        message += "%s\n" % line
    # Hack to get hostname when running via acrontab
    if not host or len(host) < 2:
        p = subprocess.Popen(['hostname'], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        out, err = p.communicate()
        host = out
        if verbose:
            print "Hostname empty, getting it from shell"
            print "Host:",host
    # echo %msg | mail -s 'HOST proxy status' MAIL_ADD
    command = " echo \"%s\" | " % (message)            
    command += "mail -s '%s: Proxy status'" % (host)
    command += " %s" % (mail)
    
    if verbose:
        print "Running email command"
        print command
    c = os.system(command)
    if verbose:
        print "Exit code:",c

def processTimeLeft(host, sendMail, verbose, proxyInfo, time, mail):
    """
    Receive the whole proxy info and return its time left.
    In case no proxy information is provided, it sends an
    email warning the user.
    """ 
    if len(proxyInfo):
        if verbose:
            print 'Proxy information:'
            for line in proxyInfo:
                print line
        timeLeft = []
        for line in proxyInfo:
            if line.find('timeleft') > -1:
                dateReg = re.compile('\d{1,3}[:/]\d{2}[:/]\d{2}')
                timeLeft = dateReg.findall(line)[0]
                timeLeft = timeLeft.split(':')[0]
                continue
    else:
        msg  = "Valid proxy file in %s not found. " % host
        msg += "Please create one.\n"

        if verbose:
            print msg
            print 'Send mail: %s' % sendMail

        if sendMail:
            if verbose:
                print 'Sending mail notification'
            sendMailNotification(mail, msg)
        sys.exit(4)   

    ### // build message
    if int(time) >= int(timeLeft):
        msg  = "\nProxy file in %s is about to expire. " % host
        msg += "Please renew it.\n"
        msg += "Hours left: %i\n" % int(timeLeft)
        if int(timeLeft) == 0:
            msg  = "Proxy file in %s HAS expired." % host
            msg += "Please renew it.\n"

        if verbose:
            print msg
            print 'Send mail: %s' % sendMail

        ### // Sends an email
        if sendMail :
            if verbose:
                print 'Sending mail notification'
            sendMailNotification(mail, msg, proxyInfo, verbose)

if __name__ == '__main__':
    main(sys.argv[1:])
