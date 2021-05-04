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

prepare_using_maven() {
    artifact_id=$1      # the artifact_id
    version=$2          # the version of the artifact, e.g. 3.9-SNAPSHOT
    release_repo=$3     # the repo containing the releases
    snapshot_repo=$4    # the repo containing the snapshots

    destination=${local_install_dir}/maven-${version}
    mkdir -p ${destination}

    # get the configured local Maven repository
    pushd ${local_install_dir} > /dev/null
    cp -r ${SIMULATOR_HOME}/conf/mvnw/.mvn .

    local_repository_path=`${SIMULATOR_HOME}/conf/mvnw/mvnw help:evaluate -Dexpression=settings.localRepository -q -DforceStdout`

    rm -r .mvn
    popd > /dev/null

    hazelcast_jar="$local_repository_path/com/hazelcast/${artifact_id}/${version}/${artifact_id}-${version}.jar"

    # we first have a look at the local Maven repository, so it's easy to provide a custom built version
    if [[ "${SKIP_LOCAL_MAVEN_REPO_LOOKUP}" == "false" ]] ; then
        echo "Searching for $hazelcast_jar in local Maven repo ($local_repository_path)"
        if [[ -f "${hazelcast_jar}" ]] ; then
            echo "Found $hazelcast_jar in local Maven repo, copying to $destination"
            cp ${hazelcast_jar} ${destination}
            return
        fi
    fi

    # the artifact is not found in the local repo, so we need to download it
    echo "$hazelcast_jar is not found in local Maven repo, downloading from remote repository"
    pushd ${local_install_dir} > /dev/null
    cp -r ${SIMULATOR_HOME}/conf/mvnw/.mvn .
    cp ${SIMULATOR_HOME}/conf/dependency-copy.xml pom.xml

    sed -i'' "s|@hz-repo-release|${release_repo}|" pom.xml
    sed -i'' "s|@hz-repo-snapshot|${snapshot_repo}|" pom.xml
    sed -i'' "s|@hz-artifact|${artifact_id}|" pom.xml
    sed -i'' "s|@hz-version|${version}|" pom.xml
    sed -i'' "s|@hz-output|${destination}|" pom.xml

    custom_maven_settings=""
    if [ ! -z ${CUSTOM_MAVEN_SETTINGS} ]; then
        echo "Using custom Maven settings: ${CUSTOM_MAVEN_SETTINGS}"
        custom_maven_settings="-s ${CUSTOM_MAVEN_SETTINGS}"
    fi

    # The Maven Dependency plugin actually checks whether the artifact is already present and picks it if it is.
    # Therefore, it won't re-download it every time and there's no need for extra handling of this case.
    ${SIMULATOR_HOME}/conf/mvnw/mvnw $custom_maven_settings dependency:copy-dependencies -Dmaven.repo.local=${SIMULATOR_HOME}/m2 -q -U

    rm pom.xml
    rm -r .mvn
    popd > /dev/null
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

    pushd ${local_repo} > /dev/null

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

    popd > /dev/null

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

    echo "[INFO]    Upload to $public_ip started"

    remote_hz_lib=${simulator_basename}/driver-lib

    echo "Uploading Hazelcast $local_install_dir to $public_ip:$remote_hz_lib"
    ssh ${SSH_OPTIONS} ${user}@${public_ip} "mkdir -p $remote_hz_lib"

    # in the local_install_dir multiple directories could be created e.g. git=master, maven=3.8. Each of these
    # directories we want to upload; but we do not want to remove other non conflicting directories.
    for dir in $(find ${local_install_dir} -maxdepth 1 -type d); do
        # sync the directory and put the task in the background
         rsync --checksum -avv --delete -L -e "ssh $SSH_OPTIONS" ${local_install_dir}/* ${user}@${public_ip}:${remote_hz_lib}
    done

    echo "[INFO]    Upload to $public_ip completed"
}

# uploads the installation files to all agents
upload() {
    # if there are no provided public ips, then it is a local install
    if [ -z "$public_ips" ] ; then
        echo "Local install"
        mkdir -p ${SIMULATOR_HOME}/workers/${session_id}/lib

        for dir in $(find ${local_install_dir} -maxdepth 1 -type d -not -empty); do
          cp -r ${dir}/* ${SIMULATOR_HOME}/workers/${session_id}/lib
        done

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
