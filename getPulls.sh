#!/bin/bash

### It fetchs pullRequests made against a given <user> and <repository>.
### For cmsdist repo, it only fecthes those against "comp" branch.
### For deployment repo, it's going to fetch everything.
### It creates a new stg patch for every pull request.
###
### Usage: getPulls.sh -h
### Usage: getPulls.sh -r <repository> <list of pull request numbers separated by space>
### Usage: Example: bash getPulls.sh -r cmsdist 263 266 267

usage()
{
  perl -ne '/^### Usage:/ && do { s/^### ?//; print }' < $0
  exit 1
}

help()
{
  perl -ne '/^###/ && do { s/^### ?//; print }' < $0
  exit 0
}

for arg; do
  case $arg in
    -h) help ;;
    -r) REPO=$2; shift; shift ;;
    -*) usage ;;
  esac
done

cmsdist()
{
  URL="https://github.com/${USER}/${REPO}/pull"

  for PULL in $PULLS; do
    echo "Pull: $URL/$PULL"
    BRANCH=`curl -ks GET ${URL}/${PULL} | grep "cms-sw/cmsdist:branch:comp" | awk -F' ' '{print $3}' | sed 's/"//g'`
    if [ -n "$BRANCH" ]; then
      echo "$PULL against comp branch? YES"
    else
      echo "$PULL against comp branch? NO"
    fi
    authdate=`curl -ks ${URL}/${PULL}.patch | head -n4 | egrep -o 'Date:.*' | cut -d: -f 2` 
    author=`curl -ks ${URL}/${PULL}.patch | head -n4 | egrep -o 'From:.*' | cut -d: -f 2` 
    title=`curl -ks ${URL}/${PULL}.patch | head -n4 | egrep -o '[PATCH].*' | cut -d" " -f 2-`
    echo "Summary: authdate=$authdate, author=$author, title=$title"
    echo ""
  #stg new -m "${title%\.}. Close #$pullreq." pullreq-$pullreq --author "$author" --authdate "$authdate"; 
  done
}

PULLS=$@

if [ "$REPO" == "cmsdist" ]; then
  USER="cms-sw"
  cmsdist
elif [ "$REPO" == "deployment" ]; then
  USER="dmwm"
  deployment
else
  echo "$REPO I do *not* know this repository. Quitting ..."
  exit 2
fi

exit 0
