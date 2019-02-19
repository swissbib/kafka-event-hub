#!/usr/bin/env bash

SHELL=/bin/bash

TIMESTAMP=`date +%Y%m%d%H%M%S`

DOCKER_BASE=/swissbib/harvesting/docker.cc
CONFDIR=$DOCKER_BASE/configs
cd $DOCKER_BASE


DEFAULTREPOS="abn alexrepo bgr boris ecod edoc eperiodica ethresearch hemu idsbb idslu idssg kbtg libib nb posters sbt serval sgbn vaud_lib vaud_school zora nebis rero"


if [ -n "$1" ]; then
  repos=$*
  repo_force=1
else
  repos=${DEFAULTREPOS}
  repo_force=0
fi

for repo in ${repos}; do

  LOCKFILE=${CONFDIR}/${repo}.lock

  if [ -e ${LOCKFILE} ]; then
    echo -n "${repo} is locked, probably by another harvester: "
    cat ${LOCKFILE}
    continue
  else
    echo $$ >${LOCKFILE}
  fi

  $DOCKER_BASE/admin/start_cc_container.sh $repo >> /swissbib/harvesting/docker.cc/start_cc_container.log
  rm ${LOCKFILE}

done
