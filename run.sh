#!/usr/bin/env bash

file="mediaset-share-1.0.4.jar"
input_path="./target/$file"
output_path="/tmp/workspace/mediaset"
root_pom_path="."
ssh_user="julio"

msg_init="-> STARTED mvn package"
msg_end="-> FINISHED mvn package"

################################
### Empaquetado del proyecto ###
sh run_package.sh $file $input_path

################################
###     Subida al cluster    ###

msg_init="-> STARTED upload file : $file to : $output_path"
msg_end="-> FINISHED upload file : $file to : $output_path"

echo $msg_init

scp ./target/$file $ssh_user@pueworker1.pue.es:$output_path
rc=$?
if [ $rc -ne 0 ] ; then
  echo 'SCP Step FAILED !!!'; exit $rc
fi

echo $msg_end

################################
###     Ejecucion            ###
# Param hdfs
#ssh francisco@pueworker1.pue.es -- spark2-submit --master yarn --deploy-mode cluster --driver-memory 512m --executor-cores 1 --num-executors 1 --executor-memory 500m --class es.pue.mediaset.share.PrepararDatos --files hdfs:///user/francisco/mediaset-share.properties $output_path/$file --parametrization-filename "mediaset-share.properties"
# ssh francisco@pueworker1.pue.es -- spark2-submit --master yarn --deploy-mode cluster --class es.pue.mediaset.share.PrepararDatos --files hdfs:///user/francisco/mediaset-share.properties $output_path/$file --parametrization-filename "mediaset-share.properties"
# Param local filesystem
ssh $ssh_user@pueworker1.pue.es -- spark2-submit --master yarn --deploy-mode cluster --class es.pue.mediaset.share.Share --files file:///$output_path/mediaset-share.properties $output_path/$file --parametrization-filename "mediaset-share.properties"
