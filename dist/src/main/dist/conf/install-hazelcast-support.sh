#!/bin/bash

#
# Script to install Hazelcast. The installation can be modified by copying it into the working directory.
#
# All properties from the 'simultor.properties' are passed as environment variables.
#

# Automatic exit on script failure
set -e
# Printing the command being executed (useful for debugging)
#set -x

local_upload_dir=upload
ssh_options=$SSH_OPTIONS
user=$USER

# we limit the number of concurrent uploads
max_current_uploads=2

# setting the right maven executable
eval git_build_dir=$GIT_BUILD_DIR

local_build_cache=$git_build_dir/build-cache

simulator_basename=($(basename $SIMULATOR_HOME))

# we create a tmp directory for all the artifacts
# for more info about this command see:
# http://unix.stackexchange.com/questions/30091/fix-or-alternative-for-mktemp-in-os-x
tmp_dir=`mktemp -d 2>/dev/null || mktemp -d -t 'mytmpdir'`
local_install_dir="$tmp_dir/lib"
mkdir -p $local_install_dir

download()
{
    url=$1 # the url of the artifact to download
    dir=$2 # the directory the file should be downloaded

    if type "wget" >> /dev/null; then
        wget --no-verbose --directory-prefix=$dir $url
    else
        pushd .
        cd $dir
        curl -O $url
        popd
    fi
}

prepare_using_maven()
{
    artifact_id=$1      # the artifact_id
    version=$2          # the version of the artifact, e.g. 3.6
    release_repo=$3     # the repo containing the releases
    snapshot_repo=$4    # the repo containing the snapshot

    maven_repo="$HOME/.m2/repository"

    destination=$local_install_dir/maven-$version
    mkdir -p $destination

    hazelcast_jar="$maven_repo/com/hazelcast/${artifact_id}/${version}/${artifact_id}-${version}.jar"
    if [[ -f "${hazelcast_jar}" ]] ; then
        # first we look in the local repo
        echo "Found $hazelcast_jar in local repo, to $destination"
        cp $hazelcast_jar $destination
        return
    fi

    # The artifact is not found in the local repo; so now we need to download it.

    echo "$hazelcast_jar is not found in local repo; downloading"
    url=""
    if  [[ $version == *SNAPSHOT ]] ; then
        # it is a snapshot. These are a bit more complex because we need to download maven-metadata first
        # to figure out the correct url

        download $snapshot_repo/com/hazelcast/$artifact_id/$version/maven-metadata.xml $local_install_dir

        version_no_snapshot=${version%-SNAPSHOT}
        buildNumber=($(grep -o -P '(?<=buildNumber>).*(?=</buildNumber)' $local_install_dir/maven-metadata.xml))
        timeStamp=($(grep -o -P '(?<=timestamp>).*(?=</timestamp)' $local_install_dir/maven-metadata.xml))

        # cleanup the garbage
        rm $local_install_dir/maven-metadata.xml

        url=$snapshot_repo/com/hazelcast/$artifact_id/$version/$artifact_id-$version_no_snapshot-$timeStamp-$buildNumber.jar
    else
        url=$release_repo/com/hazelcast/$artifact_id/$version/$artifact_id-$version.jar
    fi

    echo "Copying $url to $destination"
    download $url  $destination
}

prepare_using_git()
{
    git_branch=$1       # the branch to check out
    local_repo=$2       # the local directory to check out to
    repo_url=$3         # the remote repository

    echo Git using $git_branch

    #first we make sure the repo is cloned.
    if [ -d $local_repo ]; then
        echo "Local git repo $local_repo exist skipping clone"
    else
        echo "Local git repo $local_repo does not exist, cloning $repo_url"
        mkdir -p $git_build_dir
        git clone $repo_url $local_repo
    fi

    pushd .

    cd $local_repo

    # todo: for enterprise you need different set or repo's
    # make sure all remote repo's are added.
    for remote in ${GIT_CUSTOM_REPOSITORIES//,/ } ; do
        name=${remote%%=*}
        url=${remote#*=}

    done

    git fetch --all --tags
    git reset --hard
    git checkout $git_branch

    hash=($(git rev-parse HEAD))

    #getting a cached artifact, or else build it
    if [ -d $local_build_cache/$hash ]; then
        echo "$hash cached version available, skipping build"
    else
        echo "$hash cached version not available, building"

        if [ -z "$MVN_EXECUTABLE" ]; then mvn="mvn"; else eval mvn=$MVN_EXECUTABLE; fi

        $mvn --quiet clean package -DskipTests
        mkdir -p $local_build_cache/$hash

        jars=($(find . -name *.jar | grep --invert-match 'sources\|tests\|original\|build-utils/'))
        for jar in "${jars[@]}" ; do
            cp $jar $local_build_cache/$hash
        done

        $mvn clean

        echo "$hash build and cached successfully"
    fi

    popd

    # we need to encode the git_branch so it deals with slashes correctly
    encoded_git_branch=${git_branch//\//\\\\}

    # copy from the cache into the local_install_dir
    destination=$local_install_dir/git-${encoded_git_branch}
    mkdir -p $destination
    jars=($(find $local_build_cache/$hash -name *.jar))
    for jar in "${jars[@]}" ; do
        echo "Copying file $jar to $destination"
        cp $jar $destination
    done
}

# throttles the number of concurrent uploads by limiting the number of child processes
throttle_concurrent_uploads() {
    while test $(jobs -p | wc -w) -ge "$max_current_uploads"; do
        wait -n || true # -n is not supported on older bash versions; so we can't throttle there
    done
}

# uploads the files to a single agent
upload_to_remote_agent(){
    public_ip=$1

    remote_hz_lib=$simulator_basename/hz-lib
    remote_run_dir=$simulator_basename/workers/$session_id
    remote_lib_dir=$remote_run_dir/lib/
    remote_upload_dir=$remote_run_dir/upload

    # if the local upload directory exist, it needs to be uploaded
    if [ -d $local_upload_dir ]; then
        echo Uploading upload directory $local_upload_dir to $public_ip:remote_upload_dir
        ssh $ssh_options $user@$public_ip "mkdir -p $remote_upload_dir"
        scp $ssh_options -r $local_upload_dir simulator@${public_ip}:${remote_upload_dir}
    fi

    echo Uploading Hazelcast $local_install_dir to $public_ip:$remote_hz_lib
    ssh $ssh_options $user@$public_ip "mkdir -p $remote_hz_lib"

    # in the local_install_dir multiple directories could be created e.g. git=master, maven=3.8. Each of these
    # directories we want to upload; but we do not want to remove other non conflicting directories.
    for dir in $(find $local_install_dir -maxdepth 1 -type d); do
        # sync the directory and put the task in the background
         rsync --checksum -avv --delete -L -e "ssh $ssh_options" $local_install_dir/* $user@$public_ip:$remote_hz_lib
    done
}

upload_to_local_agent(){
    echo "Local install"

    mkdir -p $SIMULATOR_HOME/workers/$session_id/lib
    cp -r $local_install_dir $SIMULATOR_HOME/workers/$session_id/lib

    # if the local upload directory exist, it needs to be uploaded
    if [ -d $local_upload_dir ]; then
       echo "Uploading 'upload' directory"
       cp -r $local_upload_dir $SIMULATOR_HOME/workers/$session_id/upload
    fi
}
# uploads the installation files to all agents
upload()
{
    # if there are no provided public ip's, then it is a local install
    if [ -z "$public_ips" ] ; then
        upload_to_local_agent
    else
        # it is a remote install; so upload to each of the public ip's
        # the public_ips is a comma separated list
        # we execute the uploading in parallel
        for public_ip in ${public_ips//,/ } ; do
            #throttle_concurrent_uploads
            upload_to_single_agent $public_ip &
        done

        # wait for all uploads to complete.
        wait
    fi
}
