#!/bin/bash

declare -a stage
stage[1]=t1joinOutput
stage[2]=countOutput
stage[3]=sortOutput
stage[4]=maxOutput
stage[5]=tagOutput
stage[6]=tagSortOutput
stage[7]=tagMaxOutput
stage[8]=t1finalOutput

#A1_HOME=/usr/local/Cellar/hadoop/a1/t1
declare -i counter

declare -i numOfArgs
numOfArgs=$#

if [ ${numOfArgs}  -eq  0 ]; then
    echo Usage : Assignment1 \<stage\>

else

counter=$1;

while [ ${counter} -le 2 ]
    do
 #   rm -r -d ${A1_HOME}/${stage[${counter}]}
    hdfs dfs -rm -r ${stage[${counter}]}

    counter=$(( $counter+1 ))
    done

counter=$1;
hadoop jar ./t1.jar MRDriver ${counter}



#while [ ${counter} -le 8 ]
#    do
#    mkdir ${A1_HOME}/${stage[${counter}]}
#    hdfs dfs -copyToLocal ${stage[${counter}]} ${A1_HOME}/${stage[${counter}]}
#
#    counter=$(( $counter+1 ))
#    done

    fi












