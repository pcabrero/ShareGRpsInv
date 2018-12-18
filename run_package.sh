#!/usr/bin/env bash

file=$1
input_path=$2

echo $file
echo $input_path

msg_init="-> STARTED mvn package"
msg_end="-> FINISHED mvn package"

echo $msg_init

################################
### Empaquetado del proyecto ###
# mvn package # Uncomment this if you want to package it locally without docker
docker run -it --rm --name mediaset -v "$PWD":/usr/src/mymaven -v "$HOME/.m2":/root/.m2 -v "$PWD/target:/usr/src/mymaven/target" -w /usr/src/mymaven maven:3.6.0-jdk-8-alpine mvn package
rc=$?
if [ $rc -ne 0 ] ; then
  echo 'MVN PACKAGE Step FAILED !!!'; exit $rc
fi

echo $msg_end