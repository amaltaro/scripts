#!/bin/bash

BASE_DIR=/data/srv 
DEPLOY_DIR=$BASE_DIR/wmagent 
ENV_FILE=/data/admin/wmagent/env.sh 
MANAGE=/data/srv/wmagent/current/config/wmagent/ 
PYTHON_WMA_DIR=$DEPLOY_DIR/v$WMA_TAG/sw.pre/$WMA_ARCH/cms/wmagent/$WMA_TAG/lib/python2.6/site-packages 

# TODO: change these values before deploying
CMSWEB_TAG=HG1401h 
TEAMNAME=testbed-production
OP_EMAIL=alan.malta@cern.ch
WMA_TAG=0.9.91

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

echo "*** Starting services ***"
./manage start-services
echo "Done!" && echo
sleep 5

echo "*** Initializing the agent ***"
./manage init-agent
echo "Done!" && echo
sleep 5

###
# TODO: You can apply patches here
###
# echo "*** Applying patches ***"
# patch -d $PYTHON_WMA_DIR -p 3 < /data/cmsprod/dbs3_T0_patch1.patch
###

###
# tweak configuration
###
echo "*** Tweaking configuration ***"
sed -i "s+team1,team2,cmsdataops+$TEAMNAME+" $MANAGE/config.py
sed -i "s+OP EMAIL+$OP_EMAIL+" $MANAGE/config.py
sed -i "s+config.PhEDExInjector.diskSites = \[\]+config.PhEDExInjector.diskSites = \['storm-fe-cms.cr.cnaf.infn.it','srm-cms-disk.gridpp.rl.ac.uk','cmssrm-kit.gridka.de','ccsrm.in2p3.fr'\]+" $MANAGE/config.py
sed -i "s+'Running': 169200, 'Pending': 360000, 'Error': 1800+'Running': 169200, 'Pending': 2592000, 'Error': 1800+" $MANAGE/config.py
echo "Done!" && echo

#sed -i "s+LsfPluginJobGroup = '/groups/tier0/wmagent_testing'+LsfPluginJobGroup = '/groups/tier0/hufnagel/vocms104'+" ./config/tier0/config.py
#sed -i "s+LsfPluginBatchOutput = 'None'+LsfPluginBatchOutput = '/afs/cern.ch/user/h/hufnagel/scratch0/tier0_logs'+" ./config/tier0/config.py
#sed -i "s+cmsrepack+cmst0+g" ./config/tier0/config.py

###
# set resource thresholds
###
# ./manage execute-agent wmagent-resource-control --add-all-sites  --plugin=CondorPlugin
# ./manage execute-agent wmagent-resource-control --add-all-sites  --plugin=CondorPlugin --pending-slots=0 --running-slots=0

echo "*** Populating resource-control ***"
cd $MANAGE
./manage execute-agent wmagent-resource-control --site-name=T2_CH_CERN --cms-name=T2_CH_CERN --se-name=srm-eoscms.cern.ch --ce-name=T2_CH_CERN --pending-slots=1500 --running-slots=4000 --plugin=CondorPlugin
./manage execute-agent wmagent-resource-control --site-name=T2_CH_CERN --task-type=Processing --pending-slots=1500 --running-slots=4000
./manage execute-agent wmagent-resource-control --site-name=T2_CH_CERN --task-type=Production --pending-slots=1500 --running-slots=4000
./manage execute-agent wmagent-resource-control --site-name=T2_CH_CERN --task-type=Merge --pending-slots=100 --running-slots=100
./manage execute-agent wmagent-resource-control --site-name=T2_CH_CERN --task-type=Harvesting --pending-slots=10 --running-slots=20
./manage execute-agent wmagent-resource-control --site-name=T2_CH_CERN --task-type=Cleanup --pending-slots=100 --running-slots=100
./manage execute-agent wmagent-resource-control --site-name=T2_CH_CERN --task-type=LogCollect --pending-slots=100 --running-slots=100
./manage execute-agent wmagent-resource-control --site-name=T2_CH_CERN --task-type=Skim --pending-slots=100 --running-slots=100

./manage execute-agent wmagent-resource-control --site-name=T1_US_FNAL --cms-name=T1_US_FNAL --ce-name=T1_US_FNAL --se-name=cmssrm.fnal.gov --plugin=CondorPlugin --pending-slots=2000 --running-slots=5000
./manage execute-agent wmagent-resource-control --site-name=T1_US_FNAL --task-type=Processing --pending-slots=2000 --running-slots=4000
./manage execute-agent wmagent-resource-control --site-name=T1_US_FNAL --task-type=Production --pending-slots=2000 --running-slots=4000
./manage execute-agent wmagent-resource-control --site-name=T1_US_FNAL --task-type=Merge --pending-slots=100 --running-slots=100
./manage execute-agent wmagent-resource-control --site-name=T1_US_FNAL --task-type=Harvesting --pending-slots=10 --running-slots=20
./manage execute-agent wmagent-resource-control --site-name=T1_US_FNAL --task-type=Cleanup --pending-slots=100 --running-slots=100
./manage execute-agent wmagent-resource-control --site-name=T1_US_FNAL --task-type=LogCollect --pending-slots=100 --running-slots=100
./manage execute-agent wmagent-resource-control --site-name=T1_US_FNAL --task-type=Skim --pending-slots=100 --running-slots=100

echo "Deployment finished!" && echo
