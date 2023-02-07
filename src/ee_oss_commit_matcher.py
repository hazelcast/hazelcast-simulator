"""
An enterprise snapshot jar is published to the maven repository in case of two cases: 
1. A new commit is pushed to the enterprise repo
2. A new commit is pushed to the OSS repo. Note that, this will trigger the EE snapshot deploy, but 
still the dependency is from EE to OSS. Therefore, latest EE commit will be used with the latest OSS
commit, that is just pushed, to build the EE jar.

An OSS snapshot jar is published to the maven repository in case a new commit is pushed to the oss repo.

If a commit is pushed to Hazelcast EE master, it will use the latest master commit from the EE repo and
publish a snapshot jar to maven for latest OSS version and latest EE version.

If a commit is pushed to a OSS branch, say 5.0.z, it will only create OSS snapshot for the latest version 
in the branch.

Note that, typically only the latest patch version snapshots will be available in the maven repository 
for a minor version.
"""

from perfregtest_cli import get_project_version
import git as gitpy
import os

def validate_dir(path):
    path = os.path.expanduser(path)

    if not os.path.exists(path):
        raise Exception(f"Directory [{path}] does not exist")

    if not os.path.isdir(f"{path}/"):
        raise Exception(f"Directory [{path}] is not a directory")

    return path

def validate_git_dir(path):
    path = validate_dir(path)

    if not path.endswith("/.git"):
        corrected_path = f"{path}/.git"
        return validate_git_dir(corrected_path)

    if not os.path.exists(f"{path}/refs"):
        raise(f"Directory [{path}] is not valid git directory")
        exit(1)

    return path

"""
Returns the corresponding EE commit given a OSS commit.

Beware that this method will delete any staged changes in the OSS or EE repo.
"""
def find_ee_commit(oss_repo_path: str, ee_repo_path: str, oss_commit_hash: str) -> str:
    return find_corresponding_commit(oss_repo_path, ee_repo_path, oss_commit_hash, True)

"""
Returns the corresponding OSS commit given a EE commit.

Beware that this method will delete any staged changes in the OSS or EE repo.
"""
def find_os_commit(oss_repo_path: str, ee_repo_path: str, ee_commit_hash: str) -> str:
    return find_corresponding_commit(oss_repo_path, ee_repo_path, ee_commit_hash, False)

"""
Returns the corresponding EE commit given a OSS commit, or vice versa.

Beware that this method will delete any staged changes in the OSS or EE repo.
"""
def find_corresponding_commit(oss_repo_path: str, ee_repo_path: str, commit_hash: str, find_ee_commit: bool) -> str:
    target_remote = None
    oss_repo_valid = False
    ee_repo_valid = False
    # The following call will validate the directory and throw an exception if they are not valid.
    oss_repo = gitpy.Repo(oss_repo_path)
    for remote in oss_repo.remotes:
        # There may be more than one remote in the repo, take the repo in to account that is not a fork.
        # An extra check is added in case the repo is a EE repo.
        if "hazelcast/hazelcast-enterprise" not in remote.url and "hazelcast/hazelcast" in remote.url:
            oss_repo_valid = True
            # Fetch remote so that all commits and tags are received
            remote.fetch(tags=True)
            if find_ee_commit:
                target_remote = remote
    
    # The following call will validate the directory and throw an exception if they are not valid.
    ee_repo = gitpy.Repo(ee_repo_path)
    for remote in ee_repo.remotes:
        # There may be more than one remote in the repo, take the repo in to account that is not a fork.
        if "hazelcast/hazelcast-enterprise" in remote.url:
            ee_repo_valid = True
            # Fetch remote so that all commits and tags are received
            remote.fetch(tags=True)
            if not find_ee_commit:
                target_remote = remote


    if not oss_repo_valid:
        raise Exception("Couldn't find a remote with url including 'hazelcast/hazelcast' in the oss repo, are you sure the repo you specified has correct remotes?")

    if not ee_repo_valid:
        raise Exception("Couldn't find a remote with url including 'hazelcast/hazelcast-enterprise' in the ee repo, are you sure the repo you specified has correct remotes?")

    # This is actually impossible but still checking it.
    if target_remote == None:
        raise Exception("Couldn't find the target remote, this should not have happened. Something is really wrong.")

    # Base repo is the repo that commit_hash belongs to
    if find_ee_commit: 
        base_repo = oss_repo
        target_repo = ee_repo
        base_repo_path = oss_repo_path
    else:
        base_repo = ee_repo
        target_repo = oss_repo
        base_repo_path = ee_repo_path

    base_commit = base_repo.commit(commit_hash)
    # Checkout to the base commit
    git = base_repo.git
    # This removes any staged changes
    git.checkout("-f", commit_hash)
    # Get the project version, we will use is to determine the target branch
    project_version = get_project_version(base_repo_path)

    target_ref = None
    is_release = "-" not in project_version

    # Remove any preview version related postfix from the version.
    version = project_version.split("-")[0]

    # Old way of versioning like 5.2, add .0 to the end
    if len(version.split(".")) == 2:
        version = f"{version}.0"
    
    # version should be x.y.z now.
    [major, minor, patch] = version.split(".")

    if is_release: 
        # If the version is a release, then we should use the tag as the target ref
        target_ref = f"v{version}"
    else: 
        if patch == "0":
            # If the version is x.y.0-SOMETHING, then we should use the master branch
            target_ref = f"{target_remote.name}/master"
        else:
            # If the version is x.y.z-SOMETHING, then we should use the x.y.z branch
            target_ref = f"{target_remote.name}/{major}.{minor}.z"
    

    if is_release:
        return target_repo.commit(target_ref).hexsha
    else:
        corresponding_commit = None
        # Find the corresponding commit in the target repo depending on the date
        for commit in target_repo.iter_commits(target_ref):
            if find_ee_commit:
                # Return the first commit that is after the OSS commit
                if commit.committed_date > base_commit.committed_date:
                    corresponding_commit = commit
            else:
                # Return the first commit that is before the EE commit
                if commit.committed_date < base_commit.committed_date:
                    corresponding_commit = commit
                    break
            
        if corresponding_commit == None:
            raise Exception("Couldn't find the corresponding commit")
        return corresponding_commit.hexsha
