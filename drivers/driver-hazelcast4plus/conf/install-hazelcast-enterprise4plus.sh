#!/bin/bash
#
# Script to install Hazelcast Enterprise
# 
# The installation can be modified by copying it into the working directory.
# All properties from the 'simulator.properties' are passed as environment variables.
#

. ${SIMULATOR_HOME}/drivers/driver-hazelcast4plus/conf/install-hazelcast-support4plus.sh

session_id=$1
version_spec=$2
public_ips=$3

prepare()
{
    echo VERSION_SPEC ${version_spec}

    if  [[ ${version_spec} == maven* ]] ; then
        version=${version_spec#*=}

        echo "Maven using $version"

        snapshot_repo="https://repository.hazelcast.com/snapshot"
        release_repo="https://repository.hazelcast.com/release"

        prepare_using_maven "hazelcast-enterprise-all" "$version" ${release_repo} ${snapshot_repo}
    elif [[ ${version_spec} == git* ]] ; then
        git_branch=${version_spec#*=}

        echo "Git using $git_branch"

        prepare_using_git ${git_branch} ${git_build_dir}/hazelcast-enterprise git@github.com:hazelcast/hazelcast-enterprise.git
    elif [[ ${version_spec} == bringmyown ]] ; then
        echo "Bring my own"
        # we don't need to do anything
    elif [[ ${version_spec} == outofthebox ]] ; then
        echo "Aborting, VERSION_SPEC 'outofthebox' is not supported for hazelcast-enterprise"
        exit 1
    else
        echo "Aborting, invalid VERSION_SPEC [$version_spec]"
        exit 1
    fi
}

prepare
upload
