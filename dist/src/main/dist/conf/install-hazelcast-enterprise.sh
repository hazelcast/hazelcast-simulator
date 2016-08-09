#!/bin/bash

. $SIMULATOR_HOME/conf/install-hazelcast-support.sh

testsuite_id=$1
version_spec=$2
public_ips=$3

prepare()
{
    echo HAZELCAST_ENTERPRISE_VERSION_SPEC $version_spec

    if  [[ $version_spec == maven* ]] ; then
        version=${version_spec#*=}

        echo Maven using $version

        prepare_using_maven "hazelcast-enterprise-all" "$version" \
           "https://repository-hazelcast-l337.forge.cloudbees.com/release" \
           "https://oss.sonatype.org/content/repositories/snapshots"
    elif [[ $version_spec == git* ]] ; then
        git_branch=${version_spec#*=}

        echo Git using $git_branch

        prepare_using_git $git_branch $git_build_dir/hazelcast-enterprise git@github.com:hazelcast/hazelcast-enterprise.git
    elif [[ $version_spec == bringmyown ]] ; then
        echo Bring my own
        # we don't need to do anything.
    elif [[ $version_spec == outofthebox ]] ; then
        echo "Aborting, ENTERPRISE_VERSION_SPEC 'outofthebox' is not supported for hazelcast-enterprise"
        exit 1
    else
        echo "Aborting, invalid ENTERPRISE_VERSION_SPEC $version_spec"
        exit 1
    fi
}


prepare
upload