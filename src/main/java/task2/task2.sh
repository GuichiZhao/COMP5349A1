#!/bin/bash

declare -a stage
stage[1]=photoOutput
stage[2]=placeOutput
stage[3]=joinOutput
stage[4]=groupNeighbourOutput
stage[5]=groupLocalityOutput
stage[6]=finalOutput

A1_HOME=/usr/local/Cellar/hadoop/a1/t2
declare -i counter

declare -i numOfArgs
numOfArgs=$#

if [ ${numOfArgs}  -eq  0 ]; then
    echo Usage : Assignment1 \<stage\>

else

counter=$1;

while [ ${counter} -le 6 ]
    do
    rm -r -d ${A1_HOME}/${stage[${counter}]}
    hdfs dfs -rm -r ${stage[${counter}]}

    counter=$(( $counter+1 ))
    done

counter=$1;
hadoop MRDriverTask2 ${counter}


while [ ${counter} -le 6 ]
    do
    mkdir ${A1_HOME}/${stage[${counter}]}
    hdfs dfs -copyToLocal ${stage[${counter}]} ${A1_HOME}/${stage[${counter}]}

    counter=$(( $counter+1 ))
    done

    fi












