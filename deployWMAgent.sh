#!/bin/bash

BASE_DIR=/data/srv 
DEPLOY_DIR=$BASE_DIR/wmagent 
ENV_FILE=/data/admin/wmagent/env.sh 
CURRENT=/data/srv/wmagent/current
MANAGE=/data/srv/wmagent/current/config/wmagent/ 
PYTHON_WMA_DIR=$DEPLOY_DIR/v$WMA_TAG/sw/$WMA_ARCH/cms/wmagent/$WMA_TAG/lib/python2.6/site-packages 

# TODO: change these values before deploying
CMSWEB_TAG=HG1401h 
TEAMNAME=testbed-production
OP_EMAIL=alan.malta@cern.ch
WMA_TAG=0.9.91
GLOBAL_DBS_URL=https://cmsweb-testbed.cern.ch/dbs/int/global/DBSReader 
#GLOBAL_DBS_URL=https://cmsweb.cern.ch/dbs/prod/global/DBSReader

WMA_ARCH=slc5_amd64_gcc461

mkdir -p $DEPLOY_DIR || true

cd $BASE_DIR
rm -rf deployment;
wget -O deployment.zip --no-check-certificate https://github.com/dmwm/deployment/archive/$CMSWEB_TAG.zip;
unzip -q deployment.zip && cd deployment-$CMSWEB_TAG

echo "*** Removing the current crontab ***"
/usr/bin/crontab -r;
echo "Done!" && echo

echo "*** Bootstrapping WMAgent: prep ***"
source $ENV_FILE;
(cd $BASE_DIR/deployment-$CMSWEB_TAG
./Deploy -R wmagent@$WMA_TAG -s prep -A $WMA_ARCH -t v$WMA_TAG /data/srv/wmagent wmagent) && echo

echo "*** Deploying WMAgent: sw ***"
(cd $BASE_DIR/deployment-$CMSWEB_TAG
./Deploy -R wmagent@$WMA_TAG -s sw -A $WMA_ARCH -t v$WMA_TAG /data/srv/wmagent wmagent) && echo

echo "*** Posting WMAgent: post ***"
(cd $BASE_DIR/deployment-$CMSWEB_TAG
./Deploy -R wmagent@$WMA_TAG -s post -A $WMA_ARCH -t v$WMA_TAG /data/srv/wmagent wmagent) && echo

echo "*** Activating the agent ***"
cd $MANAGE
./manage activate-agent
echo "Done!" && echo

# TODO: if the agent uses Oracle, then we need to clean up it

### Enabling couch watchdog:
echo "*** Enabling couch watchdog ***"
sed -i "s+RESPAWN_TIMEOUT=0+RESPAWN_TIMEOUT=5+" $CURRENT/sw/$WMA_ARCH/external/couchdb/*/bin/couchdb
echo "Done!" && echo

echo "*** Starting services ***"
./manage start-services
echo "Done!" && echo
sleep 5

###
# TODO: You can apply patches here
###
echo "*** Applying patches ***"
cd $CURRENT
wget https://github.com/dmwm/WMCore/pull/4954.patch -O - | patch -d apps/wmagent -p 1 # for deployment
wget https://github.com/dmwm/WMCore/pull/4959.patch -O - | patch -d apps/wmagent/lib/python2.6/site-packages -p 3   # stage in bug at FNAL
wget https://github.com/dmwm/WMCore/pull/4988.patch -O - | patch -d apps/wmagent/lib/python2.6/site-packages -p 3   # fix dbsbuffer not associating...
wget https://github.com/dmwm/WMCore/pull/5023.patch -O - | patch -d apps/wmagent/lib/python2.6/site-packages -p 3   # fix open block query
wget https://github.com/dmwm/WMCore/pull/5026.patch -O - | patch -d apps/wmagent/lib/python2.6/site-packages -p 3   # FNAL TFC change
wget https://github.com/dmwm/WMCore/pull/5038.patch -O - | patch -d apps/wmagent/lib/python2.6/site-packages -p 3   # set FRONTIER_ID
wget https://github.com/dmwm/WMCore/pull/5082.patch -O - | patch -d apps/wmagent -p 1   # drop Disk endpoints
wget https://github.com/dmwm/WMCore/commit/1dc5ba5b593dacf9c171e993972fa0035d50181e.patch -O - | patch -d apps/wmagent/lib/python2.6/site-packages/ -p 3  # fix LHEInputFiles
wget https://github.com/dmwm/WMCore/pull/5110.patch -O - | patch -d apps/wmagent/lib/python2.6/site-packages/ -p 3  # fix for PreMixing
wget https://github.com/dmwm/WMCore/pull/5117.patch -O - | patch -d apps/wmagent/lib/python2.6/site-packages/ -p 3  # fix for DQMIO tier
wget https://github.com/dmwm/WMCore/pull/5130.patch -O - | patch -d apps/wmagent/lib/python2.6/site-packages/ -p 3  # Fix Sandbox creation
wget https://github.com/dmwm/WMCore/commit/e7d6b2cc0896ef74aabbdd70094bd04db4067d5b.patch -O - | patch -d apps/wmagent/lib/python2.6/site-packages/ -p 3  # includes checks for CVMFS detection
#wget https://github.com/dmwm/WMCore/commit/e7d6b2cc0896ef74aabbdd70094bd04db4067d5b.patch -O - | patch -d apps/wmagent -p 1
wget https://github.com/dmwm/WMCore/pull/5198.patch -O - | patch -d apps/wmagent/lib/python2.6/site-packages/ -p 3  # add check for non-existing jobAd
wget https://github.com/dmwm/WMCore/pull/5199.patch -O - | patch -d apps/wmagent/lib/python2.6/site-packages/ -p 3  # Fixes a bug in condor plugin
cd -
echo "Done!" && echo

echo "*** Initializing the agent ***"
./manage init-agent
echo "Done!" && echo
sleep 5

###
# tweak configuration
###
echo "*** Tweaking configuration ***"
sed -i "s+team1,team2,cmsdataops+$TEAMNAME+" $MANAGE/config.py
sed -i "s+OP EMAIL+$OP_EMAIL+" $MANAGE/config.py
sed -i "s+ErrorHandler.maxRetries = 3+ErrorHandler.maxRetries = 0+" $MANAGE/config.py
sed -i "s+config.PhEDExInjector.diskSites = \[\]+config.PhEDExInjector.diskSites = \['storm-fe-cms.cr.cnaf.infn.it','srm-cms-disk.gridpp.rl.ac.uk','cmssrm-kit.gridka.de','ccsrm.in2p3.fr','cmssrmdisk.fnal.gov'\]+" $MANAGE/config.py
sed -i "s+'Running': 169200, 'Pending': 360000, 'Error': 1800+'Running': 169200, 'Pending': 259200, 'Error': 1800+" $MANAGE/config.py
### the sed below actually is a bug fix for #4968
sed -i "s+config.DBSInterface.globalDBSUrl = 'https://cmsdbsprod.cern.ch:8443/cms_dbs_prod_global_writer/servlet/DBSServlet'+config.DBSInterface.globalDBSUrl = '$GLOBAL_DBS_URL'+" $MANAGE/config.py
sed -i "s+config.DBSInterface.DBSUrl = 'https://cmsdbsprod.cern.ch:8443/cms_dbs_prod_global_writer/servlet/DBSServlet'+config.DBSInterface.DBSUrl = '$GLOBAL_DBS_URL'+" $MANAGE/config.py
echo "Done!" && echo

###
# set resource thresholds
###
echo "*** Populating resource-control ***"
cd $MANAGE
#./manage execute-agent wmagent-resource-control --add-all-sites  --plugin=CondorPlugin --pending-slots=50 --running-slots=50
echo "\$manage execute-agent wmagent-resource-control --add-T1s --plugin=CondorPlugin --pending-slots=50 --running-slots=50"
./manage execute-agent wmagent-resource-control --add-T1s --plugin=CondorPlugin --pending-slots=50 --running-slots=50
echo "\$manage execute-agent wmagent-resource-control --add-T2s --plugin=CondorPlugin --pending-slots=50 --running-slots=50"
./manage execute-agent wmagent-resource-control --add-T2s --plugin=CondorPlugin --pending-slots=50 --running-slots=50
#./manage execute-agent wmagent-resource-control --site-name=T2_CH_CERN --cms-name=T2_CH_CERN --se-name=srm-eoscms.cern.ch --ce-name=T2_CH_CERN --pending-slots=1500 --running-slots=4000 --plugin=CondorPlugin
#./manage execute-agent wmagent-resource-control --site-name=T2_CH_CERN --task-type=Processing --pending-slots=1500 --running-slots=4000
#./manage execute-agent wmagent-resource-control --site-name=T2_CH_CERN --task-type=Production --pending-slots=1500 --running-slots=4000
#./manage execute-agent wmagent-resource-control --site-name=T2_CH_CERN --task-type=Merge --pending-slots=100 --running-slots=100
#./manage execute-agent wmagent-resource-control --site-name=T2_CH_CERN --task-type=Harvesting --pending-slots=10 --running-slots=20
#./manage execute-agent wmagent-resource-control --site-name=T2_CH_CERN --task-type=Cleanup --pending-slots=100 --running-slots=100
#./manage execute-agent wmagent-resource-control --site-name=T2_CH_CERN --task-type=LogCollect --pending-slots=100 --running-slots=100
#./manage execute-agent wmagent-resource-control --site-name=T2_CH_CERN --task-type=Skim --pending-slots=100 --running-slots=100
#
#./manage execute-agent wmagent-resource-control --site-name=T1_US_FNAL --cms-name=T1_US_FNAL --ce-name=T1_US_FNAL --se-name=cmssrm.fnal.gov --plugin=CondorPlugin --pending-slots=2000 --running-slots=5000
#./manage execute-agent wmagent-resource-control --site-name=T1_US_FNAL --task-type=Processing --pending-slots=2000 --running-slots=4000
#./manage execute-agent wmagent-resource-control --site-name=T1_US_FNAL --task-type=Production --pending-slots=2000 --running-slots=4000
#./manage execute-agent wmagent-resource-control --site-name=T1_US_FNAL --task-type=Merge --pending-slots=100 --running-slots=100
#./manage execute-agent wmagent-resource-control --site-name=T1_US_FNAL --task-type=Harvesting --pending-slots=10 --running-slots=20
#./manage execute-agent wmagent-resource-control --site-name=T1_US_FNAL --task-type=Cleanup --pending-slots=100 --running-slots=100
#./manage execute-agent wmagent-resource-control --site-name=T1_US_FNAL --task-type=LogCollect --pending-slots=100 --running-slots=100
#./manage execute-agent wmagent-resource-control --site-name=T1_US_FNAL --task-type=Skim --pending-slots=100 --running-slots=100

echo && echo "Deployment finished!! However you still need to:"
echo "  1) Double check the configuration file (config.py)"
echo "  2) Start the agent by: ./manage start-agent"
echo && echo "Have a nice day!" && echo
