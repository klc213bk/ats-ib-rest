#!/bin/bash

set -e

echo "start to build ats-ib-rest"

mvn clean package -X -e --settings /home/steven/gitrepo/ats/ats-common/settings-ats.xml
