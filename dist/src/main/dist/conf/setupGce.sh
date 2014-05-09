#!/bin/bash

id=$1
p12File=$2

thispath=$(pwd)
me=$(whoami)

echo "usually password is: \"notasecret\""
openssl pkcs12 -in ${thispath}/${p12File} -out ${thispath}/temp1.pem -nodes
openssl rsa -in ${thispath}/temp1.pem -out ${thispath}/google.pem
rm temp1.pem

echo "

# =====================================================================
# Cloud selection
# =====================================================================
#
# currently configured for EC2, but can also be something else. For a complete listing of supported clouds,
# check the following link: http://jclouds.apache.org/reference/providers/
# aws-ec2 = Amazon EC2
# google-compute-engine = The Google Compute Engine.
#
CLOUD_PROVIDER=google-compute-engine


# =====================================================================
# Cloud credentials
# =====================================================================
#
CLOUD_IDENTITY=${id}

#
# This can either be a string containing the credentials, or the path to a file (needed for google compute engine)
#
CLOUD_CREDENTIAL=${thispath}/google.pem


# =====================================================================
# Cloud Tweaks
# =====================================================================
#
# Some clouds get overloaded with requests from jclouds, e.g. ec. With these settings you can control
# how much load you are putting on the cloud api's.
#
# time in milliseconds between cloud requests. default: 50
#
CLOUD_POLL_INITIAL_PERIOD=50

#
# maximum time between polls, default 1000
#
CLOUD_POLL_MAX_PERIOD=5000

#
# the number of machines started/terminated in 1 go. On ec2 20 seems a save amount.
#
CLOUD_BATCH_SIZE=20

#
# Just a prefix for the agent name. Different test clusters could be given different names.
#
GROUP_NAME=stabilizer-agent

# =====================================================================
# Hardware selection
# =====================================================================
#
# For the options that can be set on the spec check the following link:
# http://javadocs.jclouds.cloudbees.net/org/jclouds/compute/domain/TemplateBuilderSpec.html
#
# Operating system and version can be selected using:
#   osVersionMatches=[String]
#   os64Bit=[boolean]
#   osArchMatches=[String]
#   osDescriptionMatches=[String]
# For list of available operating systems see:
# http://javadocs.jclouds.cloudbees.net/org/jclouds/compute/domain/OsFamily.html
#
# Examples:
#       osFamily=UBUNTU,osVersionMatches=12.04
#       osFamily=CENTOS
#       osFamily=RHEL,os64Bit
#       osDescription=.*Ubuntu 12.04.*
#
# A specific AMI can also be selected using an imageId e.g.
#
#       imageId=us-east-1/ami-fb8e9292
#
# Warning:
#   Don't select an amazon image with ec2-user as default user because then the stabilizer will have problems to
#   log into that image. Will be fixed in the future.
#
# Hardware can be selected e.g.
# MACHINE_SPEC=minCores=2,minRam=2048
#
# You can also explicitly set hardware id, e.g. on ec2:
# hardwareId=m1.small.
# for a full listing of hardware id's on EC2, check the following link
# http://docs.amazonwebservices.com/AWSEC2/latest/UserGuide/index.html?instance-types.html
#
# to set a specific region, use locationId, e.g. locationId=us-east-1a. Make sure that the security group exists
# in that location.
#
MACHINE_SPEC=osFamily=CENTOS,os64Bit=true


SSH_OPTIONS=-o UserKnownHostsFile=/dev/null -o CheckHostIP=no -o StrictHostKeyChecking=no

USER=${me}

#You need to make sure that the security-group exists.
SECURITY_GROUP=open

# =====================================================================
# Hazelcast Version Configuration
# =====================================================================
#
# The workers can be configured to use a specific version of Hazelcast; so you don't need to depend on the Hazelcast
# version provided by the stabilizer, but you can override it with a specific version.
#
# The Hazelcast version can be configured in different ways:
#   none                    : if you worker is going to get maven installed through worker dependencies, for
#                             for more information checkout out the --workerClassPath setting on the Controller.
#   outofthebox             : if you are fine with the default configured version.
#   maven=version           : if you want to use a specific version from the maven repository, e.g.
#                                   maven=3.2
#                                   maven=3.3-SNAPSHOT
#                             Local Hazelcast artifacts will be preferred, so you can checkout e.g. an experimental
#                             branch, build the artifacts locally. Then these artifacts will be picked up.
#   path=dir                : if you have a directory containing the artifacts you want to use.
#
HAZELCAST_VERSION_SPEC=outofthebox

# =====================================================================
# JDK Installation
# =====================================================================
#
# Warning:
#   Currently only 64 bit JVM's are going to be installed if you select something else then outofthebox.
#   So make sure that your OS is 64 bits! On option to select 32/64 bits will be added in the future.
#
# The following 4 flavors are available:
#   oracle
#   openjdk
#   ibm
#   outofthebox
# out of the box is the one provided by the image. So no software is installed by the Stabilizer.
#
JDK_FLAVOR=openjdk

#
# If a 64 bits JVM should be installed. Currently only true is allowed.
#
JDK_64_BITS=true

#
# The version of java to install.
#
# Oracle supports 6/7/8
# OpenJDK supports 6/7
# IBM supports 6/7/8 (8 is an early access version)
#
# Fine grained control on the version will be added in the future. Currently is is the most recent released version.
#
JDK_VERSION=7

# =====================================================================
# Profiler configuration
# =====================================================================
#
# Warning: Yourkit only works on 32/64 bit linux distro's for the time being. No support for windows
# or mac.
#
# The worker can be configured with a profiler. The following options are
# available:
#   none
#   yourkit
# When Yourkit is enabled, a snapshot is created an put in the worker home directory. So when the artifacts
# are downloaded, the snapshots are included and can be loaded with your Yourkit GUI.
#
PROFILER=none

#
# The settings for Yourkit agent
#
# Make sure that the path matches the JVM 32/64 bits. In the future this will be automated.
#
# The libypagent.so files, which are included in Stabilizer, are for \"YourKit Java Profiler 2013\".
#
# For more information about the Yourkit setting, see:
#   http://www.yourkit.com/docs/java/help/agent.jsp
#   http://www.yourkit.com/docs/java/help/startup_options.jsp
#
YOURKIT_SETTINGS=-agentpath:\${STABILIZER_HOME}/yourkit/linux-x86-64/libyjpagent.so=dir=\${WORKER_HOME},sampling,monitors"