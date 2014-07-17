#!/bin/bash

### This script downloads a CMSWEB deployment tag and then use the Deploy script
### with the arguments provided in the command line to deploy WMAgent in a VOBox.
### It deploys the agent, apply all the required patches (which must be defined
### in the code), populate the resource-control database, apply final tweaks to
### the configuration and finally, download and create some utilitarian cronjobs.
###
### Usage: deployProd.sh -h
### Usage: deployProd.sh -w <wma_version> -c <cmsweb_tag> -s <scram_arch> -t <team_name> -n <agent_number> -f <DB_flavor>
### Usage: Example: deployProd.sh -w 0.9.95b -c HG1406e -s slc5_amd64_gcc461 -t step0 -f oracle
###
### TODO:
###  - automatize the clean up of the old agent
###  - automatize the way we fetch patches
###  - automatize crontab population
###  - distinguish maxRetries for MC and Reproc agents 
 
BASE_DIR=/data/srv 
DEPLOY_DIR=$BASE_DIR/wmagent 
ENV_FILE=/data/admin/wmagent/env.sh 
CURRENT=/data/srv/wmagent/current
MANAGE=/data/srv/wmagent/current/config/wmagent/ 
OP_EMAIL=alan.malta@cern.ch
GLOBAL_DBS_URL=https://cmsweb.cern.ch/dbs/prod/global/DBSReader
AG_NUM=0

### Usage function: print the usage of the script
usage()
{
  perl -ne '/^### Usage:/ && do { s/^### ?//; print }' < $0
  exit 1
}

### Help function: print help for this script
help()
{
  perl -ne '/^###/ && do { s/^### ?//; print }' < $0
  exit 0
}

### Cleanup function: it cleans up the oracle database
cleanup_oracle()
{
  cd $CURRENT/config/wmagent/
  cat > clean-oracle.sql << EOT
BEGIN
   FOR cur_rec IN (SELECT object_name, object_type
                        FROM user_objects
                        WHERE object_type IN
                                ('TABLE',
                                'VIEW',
                                'PACKAGE',
                                'PROCEDURE',
                                'FUNCTION',
                                'SEQUENCE'
                                ))
   LOOP
        BEGIN
        IF cur_rec.object_type = 'TABLE'
        THEN
                EXECUTE IMMEDIATE  'DROP '
                                || cur_rec.object_type
                                || ' "'
                                || cur_rec.object_name
                                || '" CASCADE CONSTRAINTS';
        ELSE
                EXECUTE IMMEDIATE  'DROP '
                                || cur_rec.object_type
                                || ' "'
                                || cur_rec.object_name
                                || '"';
        END IF;
        EXCEPTION
        WHEN OTHERS
        THEN
                DBMS_OUTPUT.put_line (   'FAILED: DROP '
                                || cur_rec.object_type
                                || ' "'
                                || cur_rec.object_name
                                || '"'
                                );
        END;
   END LOOP;
END;
/

EOT

  while true; do
    tmpf=`mktemp`
    ./manage db-prompt < clean-oracle.sql > $tmpf
    if grep -iq "PL/SQL procedure successfully completed" $tmpf; then
      break
    fi
  done
  rm -f $tmpf
  echo -e "PURGE RECYCLEBIN;\nselect tname from tab;" > purging.sql
  ./manage db-prompt < purging.sql
  rm -f clean-oracle.sql purging.sql
  echo "Done!" && echo
}

for arg; do
  case $arg in
    -h) help ;;
    -w) WMA_TAG=$2; shift; shift ;;
    -c) CMSWEB_TAG=$2; shift; shift ;;
    -s) WMA_ARCH=$2; shift; shift ;;
    -t) TEAMNAME=$2; shift; shift ;;
    -n) AG_NUM=$2; shift; shift ;;
    -f) FLAVOR=$2; shift; shift ;;
    -*) usage ;;
  esac
done

if [[ -z $WMA_TAG ]] || [[ -z $CMSWEB_TAG ]] || [[ -z $WMA_ARCH ]] || [[ -z $TEAMNAME ]] || [[ -z $FLAVOR ]]; then
  usage
  exit 1
fi

if [ "$FLAVOR" == "oracle" ] || [ "$FLAVOR" == "mysql" ]; then
  : # or should it be pass ?
else
  echo "ERROR: Backend database unknown. Choose either 'mysql' or 'oracle'."
  usage
  exit 2
fi

echo "Starting new agent deployment with the following data:"
echo " - WMAgent version: $WMA_TAG"
echo " - WMAgent Arch   : $WMA_ARCH"
echo " - CMSWEB tag     : $CMSWEB_TAG"
echo " - Team name      : $TEAMNAME"
echo " - Agent number   : $AG_NUM"
echo " - DB Flavor      : $FLAVOR" && echo

mkdir -p $DEPLOY_DIR || true
cd $BASE_DIR
rm -rf deployment deployment.zip deployment-${CMSWEB_TAG};
wget -nv -O deployment.zip --no-check-certificate https://github.com/dmwm/deployment/archive/$CMSWEB_TAG.zip;
unzip -q deployment.zip && cd deployment-$CMSWEB_TAG

echo "*** Removing the current crontab ***"
/usr/bin/crontab -r;
echo "Done!" && echo

echo "*** Bootstrapping WMAgent: prep ***"
source $ENV_FILE;
(cd $BASE_DIR/deployment-$CMSWEB_TAG
./Deploy -R wmagent@$WMA_TAG -s prep -A $WMA_ARCH -t v$WMA_TAG $DEPLOY_DIR wmagent) && echo

echo "*** Deploying WMAgent: sw ***"
(cd $BASE_DIR/deployment-$CMSWEB_TAG
./Deploy -R wmagent@$WMA_TAG -s sw -A $WMA_ARCH -t v$WMA_TAG $DEPLOY_DIR wmagent) && echo

echo "*** Posting WMAgent: post ***"
(cd $BASE_DIR/deployment-$CMSWEB_TAG
./Deploy -R wmagent@$WMA_TAG -s post -A $WMA_ARCH -t v$WMA_TAG $DEPLOY_DIR wmagent) && echo

### TODO: You have to manually add patches here
echo "*** Applying deployment patches ***"
cd $CURRENT
wget -nv https://github.com/ticoann/WMCore/commit/e30bd067d2733745e1cbb70af7488156ce484fee.patch -O - | patch -d apps/wmagent/lib/python2.6/site-packages/ -p 3  # sanitize couch url logs
wget -nv https://github.com/dmwm/WMCore/pull/5217.patch -O - | patch -d apps/wmagent/lib/python2.6/site-packages/ -p 3  # temp fix for lumi report on workloadsummary
cd -
echo "Done!" && echo

echo "*** Activating the agent ***"
cd $MANAGE
./manage activate-agent
echo "Done!" && echo

### Checking the database backend
echo "*** Cleaning up database instance ***"
if [ "$FLAVOR" == "oracle" ]; then
  cleanup_oracle
elif [ "$FLAVOR" == "mysql" ]; then
  echo "Mysql, nothing to clean up" && echo
fi

### Enabling couch watchdog:
echo "*** Enabling couch watchdog ***"
sed -i "s+RESPAWN_TIMEOUT=0+RESPAWN_TIMEOUT=5+" $CURRENT/sw/$WMA_ARCH/external/couchdb/*/bin/couchdb
echo "Done!" && echo

echo "*** Starting services ***"
./manage start-services
echo "Done!" && echo
sleep 5

echo "*** Initializing the agent ***"
./manage init-agent
echo "Done!" && echo
sleep 5

###
# tweak configuration
###
echo "*** Tweaking configuration ***"
sed -i "s+team1,team2,cmsdataops+$TEAMNAME+" $MANAGE/config.py
sed -i "s+Agent.agentNumber = 0+Agent.agentNumber = $AG_NUM+" $MANAGE/config.py
sed -i "s+OP EMAIL+$OP_EMAIL+" $MANAGE/config.py
sed -i "s+ErrorHandler.maxRetries = 3+ErrorHandler.maxRetries = \{'default' : 3, 'Harvesting' : 2, 'Merge' : 4, 'LogCollect' : 1, 'Cleanup' : 2\}+" $MANAGE/config.py
sed -i "s+config.PhEDExInjector.diskSites = \[\]+config.PhEDExInjector.diskSites = \['storm-fe-cms.cr.cnaf.infn.it','srm-cms-disk.gridpp.rl.ac.uk','cmssrm-fzk.gridka.de','ccsrm.in2p3.fr','srmcms.pic.es','cmssrmdisk.fnal.gov'\]+" $MANAGE/config.py
sed -i "s+'Running': 169200, 'Pending': 360000, 'Error': 1800+'Running': 169200, 'Pending': 259200, 'Error': 1800+" $MANAGE/config.py
echo "Done!" && echo

echo "*** Populating resource-control ***"
cd $MANAGE
echo "\$manage execute-agent wmagent-resource-control --add-all-sites --plugin=CondorPlugin --pending-slots=50 --running-slots=50"
./manage execute-agent wmagent-resource-control --add-all-sites  --plugin=CondorPlugin --pending-slots=50 --running-slots=50
echo "Done!" && echo

###
# set scripts and specific cronjobs
###
echo "*** Downloading utilitarian scripts ***"
cd $CURRENT
wget -q --no-check-certificate https://raw.githubusercontent.com/julianbadillo/WmAgentScripts/c6af3bdac2f9a59af87aac1a3375d5295c711f46/rmOldJobs.sh
wget -q --no-check-certificate https://raw.github.com/CMSCompOps/WmAgentScripts/master/updateSiteStatus.py
wget -q --no-check-certificate https://raw.github.com/CMSCompOps/WmAgentScripts/master/thresholdsFromSSB.py
echo "Done!" && echo

# TODO: find a way to populate the crontab without deleting its previous content
echo "*** Creating cronjobs for them ***"
echo -e "Copy and paste these stupid cronjobs into crontab please\n"
echo "#Update site status"
echo "*/20 * * * * (source /data/admin/wmagent/env.sh ; source /data/srv/wmagent/current/apps/wmagent/etc/profile.d/init.sh ; python /data/srv/wmagent/current/updateSiteStatus.py ) &> /tmp/updateSiteStatus.log"
echo "#Update site thresholds"
echo "*/20 * * * * (source /data/admin/wmagent/env.sh ; source /data/srv/wmagent/current/apps/wmagent/etc/profile.d/init.sh ; python /data/srv/wmagent/current/thresholdsFromSSB.py ) &> /tmp/thresholdsFromSSB.log"
echo "#remove old jobs script"
echo "10 */4 * * * source /data/srv/wmagent/current/rmOldJobs.sh &> /tmp/rmJobs.log"
echo -e "\nDone!" && echo

echo && echo "Deployment finished!! However you still need to:"
echo "  1) Source the new WMA env: source /data/admin/wmagent/env.sh"
echo "  2) Double check agent configuration: less config/wmagent/config.py"
echo "  3) Start the agent with: \$manage start-agent"
echo && echo "Have a nice day!" && echo
