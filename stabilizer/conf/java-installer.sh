#!/bin/sh

cd ~

        if [ "${YOURKIT_ENABLED}" = "true" ] ;
        then
	    echo YourKit is enabled
	    scp -P ${PORT} libyjpagent.so ${USER}@${ADDRESS}:/tmp/
	    scp -P ${PORT} environment-yourkit.sh ${USER}@${ADDRESS}:${RADARGUN_DIR}/bin/environment.sh
	    ssh ${USER}@${ADDRESS} -p ${PORT} "rm -fr ~/Snapshots"
        elif [ "${JACOCO_ENABLED}" = "true" ] ;
        then
	    echo Jacoco is enabled
            scp -P ${PORT} jacocoagent.jar ${USER}@${ADDRESS}:/tmp/
            scp -P ${PORT} environment-jacoco.sh ${USER}@${ADDRESS}:${RADARGUN_DIR}/bin/environment.sh
        else
	    echo Jacoc and Yourkit are disabled
	fi


wget -q http://ec2-54-87-52-100.compute-1.amazonaws.com/jdk-7u51-linux-x64.tar.gz
tar xfz jdk-7u51-linux-x64.tar.gz
echo "export JAVA_HOME=~/jdk1.7.0_51" >> ~/.bashrc

wget -q http://ec2-54-87-52-100.compute-1.amazonaws.com/jdk-8u5-linux-x64.tar.gz
tar xfz jdk-8u5-linux-x64.tar.gz
echo "export JAVA_HOME=~/jdk1.8.0_05" >> ~/.bashrc

echo 'export PATH=$JAVA_HOME/bin:$PATH' >> ~/.bashrc