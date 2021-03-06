#!/bin/bash

BASE_DIR=/data/srv 
DEPLOY_DIR=$BASE_DIR/wmagent 
ENV_FILE=/data/admin/wmagent/env.sh 
CURRENT=/data/srv/wmagent/current
MANAGE=/data/srv/wmagent/current/config/wmagent/ 
PYTHON_WMA_DIR=$DEPLOY_DIR/v$WMA_TAG/sw.pre/$WMA_ARCH/cms/wmagent/$WMA_TAG/lib/python2.6/site-packages 

# TODO: change these values before deploying
CMSWEB_TAG=HG1401h 
TEAMNAME=relvallsf
OP_EMAIL=alan.malta@cern.ch
WMA_TAG=0.9.91
GLOBAL_DBS_URL=https://cmsweb.cern.ch/dbs/prod/global/DBSReader

WMA_ARCH=slc5_amd64_gcc461

mkdir -p $DEPLOY_DIR || true

cd $BASE_DIR
rm -rf deployment;
#git clone git://github.com/dmwm/deployment.git && cd deployment && git reset --hard $CMSWEB_TAG
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
+### Enabling couch watchdog:
echo "*** Enabling couch watchdog ***"
sed -i "s+RESPAWN_TIMEOUT=0+RESPAWN_TIMEOUT=5+" $CURRENT/sw/$WMA_ARCH/external/couchdb/*/bin/couchdb

echo "*** Starting services ***"
./manage start-services
echo "Done!" && echo
sleep 5

###
# TODO: You can apply patches here
###
echo "*** Applying patches ***"
cd $CURRENT
wget https://github.com/dmwm/WMCore/pull/4954.patch -O - | patch -d sw/slc5_amd64_gcc461/cms/wmagent/$WMA_TAG/ -p 1 # for deployment
echo "Done!" && echo

echo "*** Initializing the agent ***"
cd $MANAGE
./manage init-agent
echo "Done!" && echo
sleep 5

###
# Tweak configuration including LSF changes
###
echo "*** Tweaking configuration ***"
sed -i "s+team1,team2,cmsdataops+$TEAMNAME+" $MANAGE/config.py
sed -i "s+OP EMAIL+$OP_EMAIL+" $MANAGE/config.py
sed -i "s+ErrorHandler.maxRetries = 3+ErrorHandler.maxRetries = {'default' : 3, 'Merge' : 4, 'LogCollect' : 2, 'Cleanup' : 2}+" $MANAGE/config.py
sed -i "s+config.PhEDExInjector.diskSites = \[\]+config.PhEDExInjector.diskSites = \['storm-fe-cms.cr.cnaf.infn.it','srm-cms-disk.gridpp.rl.ac.uk','cmssrm-kit.gridka.de','ccsrm.in2p3.fr'\]+" $MANAGE/config.py
sed -i "s+'Running': 169200, 'Pending': 360000, 'Error': 1800+'Running': 169200, 'Pending': 259200, 'Error': 1800+" $MANAGE/config.py
sed -i "s+config.DBSInterface.globalDBSUrl = 'https://cmsdbsprod.cern.ch:8443/cms_dbs_prod_global_writer/servlet/DBSServlet'+config.DBSInterface.globalDBSUrl = '$GLOBAL_DBS_URL'+" $MANAGE/config.py

sed -i "s+BossAir.pluginNames = \['CondorPlugin'\]+BossAir.pluginNames = \['LsfPlugin'\]+" $MANAGE/config.py
sed -i "s+SquaredAlgo+DefaultRetryAlgo+" $MANAGE/config.py
sed -i "s+coolOffTime = {'create': 5000, 'job': 5000, 'submit': 5000}+coolOffTime = {'create': 3600, 'job': 3600, 'submit': 3600}+" $MANAGE/config.py
sed -i '/config.JobSubmitter.pollInterval/i config.JobSubmitter.LsfPluginBatchOutput = "/afs/cern.ch/work/r/relval/lsf-wma/logs/"' $MANAGE/config.py
sed -i '/config.JobSubmitter.pollInterval/i config.JobSubmitter.LsfPluginJobGroup = "/groups/tier0/wmagent_vocms174"' $MANAGE/config.py
sed -i '/config.JobSubmitter.pollInterval/i config.JobSubmitter.LsfPluginResourceReq = "select[type==SLC5_64] rusage[pool=20000,mem=2000]"' $MANAGE/config.py
sed -i '/config.JobSubmitter.pollInterval/i config.JobSubmitter.LsfPluginQueue = "cmsrelval"' $MANAGE/config.py
echo "Done!" && echo

###
# set resource thresholds
###
echo "*** Populating resource-control ***"
cd $MANAGE
./manage execute-agent wmagent-resource-control --site-name=T2_CH_CERN --cms-name=T2_CH_CERN --se-name=srm-eoscms.cern.ch --ce-name=T2_CH_CERN --plugin=LsfPlugin  --pending-slots=500 --running-slots=500
./manage execute-agent wmagent-resource-control --site-name=T2_CH_CERN --task-type=Processing --pending-slots=500 --running-slots=500
./manage execute-agent wmagent-resource-control --site-name=T2_CH_CERN --task-type=Production --pending-slots=500 --running-slots=500
./manage execute-agent wmagent-resource-control --site-name=T2_CH_CERN --task-type=Merge --pending-slots=100 --running-slots=100
./manage execute-agent wmagent-resource-control --site-name=T2_CH_CERN --task-type=Harvesting --pending-slots=10 --running-slots=20
./manage execute-agent wmagent-resource-control --site-name=T2_CH_CERN --task-type=Cleanup --pending-slots=100 --running-slots=100
./manage execute-agent wmagent-resource-control --site-name=T2_CH_CERN --task-type=LogCollect --pending-slots=100 --running-slots=100
./manage execute-agent wmagent-resource-control --site-name=T2_CH_CERN --task-type=Skim --pending-slots=100 --running-slots=100

echo && echo "Deployment finished!! However you still need to:"
echo " 1) Double check the configuration file (config.py)"
echo " 2) Start the agent by: k5reauth -f -- \"./manage start-agent\""
echo && echo "Have a nice day!" && echo
