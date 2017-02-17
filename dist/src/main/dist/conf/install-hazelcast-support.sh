#!/bin/bash
#
# Support script to install Hazelcast.
#
# The installation can be modified by copying it into the working directory.
# All properties from the 'simulator.properties' are passed as environment variables.
#

# automatic exit on script failure
set -e
# printing the command being executed (useful for debugging)
#set -x

local_upload_dir=upload
user=${SIMULATOR_USER}

# we limit the number of concurrent uploads
max_current_uploads=2

# setting the right maven executable
eval git_build_dir=${GIT_BUILD_DIR}

local_build_cache=${git_build_dir}/build-cache

# we create a tmp directory for all the artifacts, for more info about this command see:
# http://unix.stackexchange.com/questions/30091/fix-or-alternative-for-mktemp-in-os-x
tmp_dir=`mktemp -d 2>/dev/null || mktemp -d -t 'mytmpdir'`
local_install_dir="$tmp_dir/lib"

function real_path {
    local r=$1; local t=$(readlink ${r})
    while [ ${t} ]; do
        r=$(cd $(dirname ${r}) && cd $(dirname ${t}) && pwd -P)/$(basename ${t})
        t=$(readlink ${r})
    done
    echo ${r}
}

simulator_basename=$(real_path ${SIMULATOR_HOME} | xargs basename)

download() {
    url=$1 # the url of the artifact to download
    dir=$2 # the directory the file should be downloaded

    if type "wget" >> /dev/null 2>&1; then
        wget --no-verbose --directory-prefix=${dir} ${url}
    else
        pushd .
        cd ${dir}
        curl -O ${url}
        popd
    fi
}

prepare_using_maven() {
    artifact_id=$1      # the artifact_id
    version=$2          # the version of the artifact, e.g. 3.6
    release_repo=$3     # the repo containing the releases
    snapshot_repo=$4    # the repo containing the snapshot

    maven_repo="$HOME/.m2/repository"

    destination=${local_install_dir}/maven-${version}
    mkdir -p ${destination}

    hazelcast_jar="$maven_repo/com/hazelcast/${artifact_id}/${version}/${artifact_id}-${version}.jar"
    if [[ -f "${hazelcast_jar}" ]] ; then
        # first we look in the local repo
        echo "Found $hazelcast_jar in local repo, to $destination"
        cp ${hazelcast_jar} ${destination}
        return
    fi

    # The artifact is not found in the local repo; so now we need to download it
    echo "$hazelcast_jar is not found in local repo; downloading"
    url=""
    if  [[ ${version} == *SNAPSHOT ]] ; then
        # snapshots are a bit more complex because we need to download maven-metadata first to figure out the correct url
        download ${snapshot_repo}/com/hazelcast/${artifact_id}/${version}/maven-metadata.xml ${local_install_dir}

        snapshot_version=$(sed -e 's/\s\+//g' ${local_install_dir}/maven-metadata.xml | grep -oPz '(?<=<snapshotVersion>\n<extension>jar</extension>\n<value>).*(?=</value>)')
        if [ -n ${snapshot_version} ]; then
            url=${snapshot_repo}/com/hazelcast/${artifact_id}/${version}/${artifact_id}-${snapshot_version}.jar
        else
            baseVersion=${version%-SNAPSHOT}
            timeStamp=$(grep -o -P '(?<=timestamp>).*(?=</timestamp)' ${local_install_dir}/maven-metadata.xml)
            buildNumber=$(grep -o -P '(?<=buildNumber>).*(?=</buildNumber)' ${local_install_dir}/maven-metadata.xml)
            url=${snapshot_repo}/com/hazelcast/${artifact_id}/${version}/${artifact_id}-${baseVersion}-${timeStamp}-${buildNumber}.jar
        fi

        # cleanup the garbage
        rm ${local_install_dir}/maven-metadata.xml
    else
        url=${release_repo}/com/hazelcast/${artifact_id}/${version}/${artifact_id}-${version}.jar
    fi

    echo "Copying $url to $destination"
    download ${url} ${destination}
}

prepare_using_git() {
    git_branch=$1 # the branch to check out
    local_repo=$2 # the local directory to check out to
    repo_url=$3   # the remote repository

    echo "Git using $git_branch"

    # first we make sure the repo is cloned
    if [ -d ${local_repo} ]; then
        echo "Local git repo $local_repo exist skipping clone"
    else
        echo "Local git repo $local_repo does not exist, cloning $repo_url"
        mkdir -p ${git_build_dir}
        git clone ${repo_url} ${local_repo}
    fi

    pushd .

    cd ${local_repo}

    # TODO: for enterprise you need different set or repo
    # make sure all remote repos are added
    for remote in ${GIT_CUSTOM_REPOSITORIES//,/ } ; do
        name=${remote%%=*}
        url=${remote#*=}
    done

    git fetch --all --tags
    git reset --hard
    git checkout ${git_branch}

    hash=$(git rev-parse HEAD)

    # getting a cached artifact, or else build it
    if [ -d ${local_build_cache}/${hash} ]; then
        echo "$hash cached version available, skipping build"
    else
        echo "$hash cached version not available, building"

        if [ -z "$MVN_EXECUTABLE" ]; then mvn="mvn"; else eval mvn=${MVN_EXECUTABLE}; fi

        ${mvn} --quiet clean package -DskipTests
        mkdir -p ${local_build_cache}/${hash}

        jars=($(find . -name *.jar | grep --invert-match 'sources\|tests\|original\|build-utils/'))
        for jar in "${jars[@]}" ; do
            cp ${jar} ${local_build_cache}/${hash}
        done

        ${mvn} clean

        echo "$hash build and cached successfully"
    fi

    popd

    # we need to encode the git_branch so it deals with slashes correctly
    encoded_git_branch=${git_branch//\//\\\\}

    # copy from the cache into the local_install_dir
    destination=${local_install_dir}/git-${encoded_git_branch}
    mkdir -p ${destination}
    searchPath=${local_build_cache}/${hash}
    jars=($(find ${searchPath} -name *.jar))
    for jar in "${jars[@]}" ; do
        echo "Copying file $jar to $destination"
        cp ${jar} ${destination}
    done
}

# throttles the number of concurrent uploads by limiting the number of child processes
throttle_concurrent_uploads() {
    while test $(jobs -p | wc -w) -ge "$max_current_uploads"; do
        # -n is not supported on older bash versions; so we can't throttle there
        wait -n || true
    done
}

# uploads the files to a single agent
upload_to_single_agent() {
    public_ip=$1

    remote_hz_lib=${simulator_basename}/hz-lib
    remote_run_dir=${simulator_basename}/workers/${session_id}
    remote_upload_dir=${remote_run_dir}/upload

    # if the local upload directory exist, it needs to be uploaded
    if [ -d ${local_upload_dir} ]; then
        echo "Uploading upload directory $local_upload_dir to $public_ip:$remote_upload_dir"
        ssh ${SSH_OPTIONS} ${user}@${public_ip} "mkdir -p $remote_upload_dir"
        scp ${SSH_OPTIONS} -r ${local_upload_dir} ${user}@${public_ip}:${remote_upload_dir}
    fi

    echo "Uploading Hazelcast $local_install_dir to $public_ip:$remote_hz_lib"
    ssh ${SSH_OPTIONS} ${user}@${public_ip} "mkdir -p $remote_hz_lib"

    # in the local_install_dir multiple directories could be created e.g. git=master, maven=3.8. Each of these
    # directories we want to upload; but we do not want to remove other non conflicting directories.
    for dir in $(find ${local_install_dir} -maxdepth 1 -type d); do
        # sync the directory and put the task in the background
         rsync --checksum -avv --delete -L -e "ssh $SSH_OPTIONS" ${local_install_dir}/* ${user}@${public_ip}:${remote_hz_lib}
    done
}

# uploads the installation files to all agents
upload() {
    # if there are no provided public ips, then it is a local install
    if [ -z "$public_ips" ] ; then
        echo "Local install"
        mkdir -p ${SIMULATOR_HOME}/workers/${session_id}/lib
        cp -r ${local_install_dir} ${SIMULATOR_HOME}/workers/${session_id}/lib
        return
    fi

    # it is a remote install; so upload to each of the public ips
    # the public_ips is a comma separated list
    # we execute the uploading in parallel
    for public_ip in ${public_ips//,/ } ; do
        # throttle_concurrent_uploads
        upload_to_single_agent ${public_ip} &
    done

    # wait for all uploads to complete
    wait
}
