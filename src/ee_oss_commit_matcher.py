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

from simulator.util import validate_git_dir
from perfregtest_cli import get_project_version
import git as gitpy

"""
Returns the corresponding EE commit given a OSS commit.

The matching will be based on date and only commit based, tags and 
releases won't be taken into account.
The commits will be retrieved from the master branch.
"""
def find_ee_commit(oss_git_dir_path: str, ee_git_dir_path: str, oss_commit_hash: str) -> str:
    return find_corresponding_commit(oss_git_dir_path, ee_git_dir_path, oss_commit_hash, True)

"""
Returns the corresponding OSS commit given a EE commit.

The matching will be based on date and only commit based, tags and 
releases won't be taken into account.
The commits will be retrieved from the master branch.
"""
def find_os_commit(oss_git_dir_path: str, ee_git_dir_path: str, ee_commit_hash: str) -> str:
    return find_corresponding_commit(oss_git_dir_path, ee_git_dir_path, ee_commit_hash, False)

"""
Returns the corresponding EE commit given a OSS commit, or vice versa.

The matching will be based on date and only commit based, tags and 
releases won't be taken into account.
The commits will be retrieved from the master branch.
"""
def find_corresponding_commit(oss_git_dir_path: str, ee_git_dir_path: str, commit_hash: str, find_ee_commit: bool) -> str:
    validate_git_dir(oss_git_dir_path)
    validate_git_dir(ee_git_dir_path)

    target_remote = None
    oss_repo = gitpy.Repo(oss_git_dir_path)
    for remote in oss_repo.remotes:
        if "hazelcast/hazelcast" in remote.url and find_ee_commit:
            target_remote = remote
            remote.fetch()
    
    ee_repo = gitpy.Repo(ee_git_dir_path)
    for remote in ee_repo.remotes:
        if "hazelcast/hazelcast-enterprise" in remote.url and not find_ee_commit:
            target_remote = remote
            remote.fetch()

    # Base repo is the repo that commit hash belongs to
    if find_ee_commit: 
        base_repo = oss_repo
        target_repo = ee_repo
        base_repo_path = oss_git_dir_path
    else:
        base_repo = ee_repo
        target_repo = oss_repo
        base_repo_path = ee_git_dir_path

    base_commit = base_repo.commit(commit_hash)
    # Checkout to the base commit commit
    git = base_repo.git
    git.checkout("-f", commit_hash)
    # reset the index and working tree to match the pointed-to commit
    base_repo.head.reset(index=True, working_tree=True) 
    project_version = get_project_version(base_repo_path)

    target_ref = None
    is_snapshot = project_version.endswith("-SNAPSHOT")
    is_tag = not is_snapshot

    if is_snapshot:
        version = project_version.split("-")[0]
    else:
        version = project_version

    # Old way of versioning like 5.2
    if len(version.split(".")) == 2:
        version = f"{version}.0"
    
    [major, minor, patch] = version.split(".")

    # version should be x.y.z now.
    if not is_snapshot: 
        # If the version is x.y.z, then we should use the vx.y.z tag
        target_ref = f"v{version}"
    else: 
        if patch == "0":
            # If the version is x.y.0-SNAPSHOT, then we should use the master branch
            target_ref = f"{target_remote.name}/master"
        else:
            # If the version is x.y.z-SNAPSHOT, then we should use the x.y.z branch
            target_ref = f"{target_remote.name}/{major}.{minor}.z"
    

    if is_tag:
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
