#!/bin/bash

profile=$1
clusterSz=$2
jenkinsMode=$3

duration=0m

boxCount=0
members=0
workers=0

addJcacheTests=1

case $profile in
    final)
        duration=48h
        ;;

    earlyaccess)
        duration=12h
        ;;

    releasecandidate)
        duration=8h
        ;;

    nightly)
        duration=6h
        ;;

    test)
        duration=1m
        ;;
    *)
        echo "Unknown profile"
        exit 1
esac

case $clusterSz in
    test)
        boxCount=2
        members=2
        workers=2
        ;;

    small)
        boxCount=4
        members=4
        workers=8
        ;;

    medium)
        boxCount=6
        members=6
        workers=24
        ;;

    large)
        boxCount=10
        members=10
        workers=50
        ;;

    xlarge)
        boxCount=25
        members=25
        workers=100
        ;;

    *)
        echo "Unknown cluster size"
        exit 1
esac


mkdir -p ${profile}/${clusterSz}

cp stabilizer.properties ${profile}/${clusterSz}
cp test.properties ${profile}/${clusterSz}
cp run.sh ${profile}/${clusterSz}

if ((addJcacheTests == 1)) ; then
    cat jcacheTest.properties >> ${profile}/${clusterSz}/test.properties
fi

output="$(pwd)/${profile}/${clusterSz}/archive/$(date '+%Y_%m_%d-%H_%M_%S')"


cd ${profile}/${clusterSz}
case $jenkinsMode in
    jenkins)

        hzVersion=$(./update-hazelcast.sh)
        if ! $? ; then
            exit 1
        fi
        sed -i s/maven=.*/maven="${hzVersion}"/g ${workers}/${duration}/stabilizer.properties
        ./run.sh ${boxCount} ${members} ${workers} ${duration} ${output}
        retCode=$?
        ;;
    *)
        nohup ./run.sh ${boxCount} ${members} ${workers} ${duration} ${output} &
        retCode=$?
esac
cd ..

exit ${retCode}
