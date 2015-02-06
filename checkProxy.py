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

def main(argv):
    """
    This is an utility that checks the proxy validity and sends an email in
    case the time left is lesser than --time.
    Arguments:
    --proxy     : Proxy file location (Default: X509_USER_PROXY)
    --mail      : Mail where notification should be sent (Default: USER).
    --send-mail : <True|False> Send mail notification (Default: True).
    --time      : Minimun time left [hours]. It should be an integer. (Default: 48 h).
    --verbose   : Print output messages.
    --help      : Print this menu.

    Example: python /data/amaltaro/checkProxy.py --proxy /data/certs/myproxy.pem
    --time 72 --send-mail True --mail alanmalta@gmail.com,alan.malta@cern.ch --verbose
    """

    valid = ["proxy=", "mail=", "send-mail=", "time=", "verbose", "help"]
    
    proxy = os.getenv('X509_USER_PROXY')
    verbose = False
    mail = os.getenv('USER')
    sendMail = True
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
        if opt == "--send-mail":
            if arg.lower().find("false") > -1:
                sendMail = False
            if arg.lower().find("true") > -1:
                sendMail = True
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

    #  //
    # // Get proxy info
    #//
    command = ["voms-proxy-info", "-all", "-file", str(proxy)]
    p = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out, err = p.communicate()
    proxyInfo = [line for line in out.split('\n') if line]
    if len(proxyInfo):
        if verbose:
            print 'Proxy information:'
            for line in proxyInfo:
                print line
        timeLeftStr = []
        for line in proxyInfo:
            if line.find('timeleft') > -1:
                timeLeftStr.append([x.strip().strip(':').strip() \
                    for x in line.split("timeleft") if  x != ""  ])
        timeLeftArr = []
        for item in timeLeftStr:
            h = float(item[0].split(':')[0])
            m = float(item[0].split(':')[1])
            s = float(item[0].split(':')[2])
            timeLeftArr.append(h + m/60 + s/3600)
        timeLeftArr.sort()
        timeLeft = timeLeftArr[0]
        if time > timeLeft :
            msg  = "Proxy file in %s is about to expire. " % host
            msg += "Please renew it.\n"
            msg += "Hours left: %i\n\n" % int(timeLeft)
            if int(timeLeft) == 0:
                msg  = "Proxy file in %s HAS expired." % host
                msg += "Please renew it.\n"

            if verbose:
                print msg
                print 'Send mail: %s' % sendMail

            #  //
            # // Send Mail
            #//
            if sendMail :
                if verbose:
                    print 'Sending mail notification'
                sendMailNotification(mail, msg, proxyInfo)
    else:
        msg  = "Valid proxy file in %s not found. " % host
        msg += "Please create one.\n"

        if verbose:
            print msg
            print 'Send mail: %s' % sendMail

        #  //
        # // Send Mail
        #//
        if sendMail:
            if verbose:
                print 'Sending mail notification'
            sendMailNotification(mail, msg)

def sendMailNotification(mail, message, proxyInfo=''):
    host = os.getenv('HOSTNAME')
    os.chdir(os.environ['HOME'])
    messageFileName = 'proxymail.txt'

    messageFile = open(messageFileName, 'w')
    messageFile.write(message)
    for line in proxyInfo:
        messageFile.write("%s\n" % line)

    command = "mail -s '%s: Proxy status'" % (host)
    command += " %s" % (mail)
    command += " < %s" % (messageFileName)

    messageFile.close()
    os.system(command)
    os.remove(messageFileName)

if __name__ == '__main__':
    main(sys.argv[1:])
