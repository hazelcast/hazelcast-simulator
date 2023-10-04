#!/usr/bin/env python3
import os.path
import subprocess

import yaml
import sys
from simulator.util import run_parallel, shell
from simulator.hosts import public_ip, ssh_user, ssh_options
from simulator.ssh import Ssh


def __upload(agent, artifact_ids, version):
    print(f"[INFO]     {public_ip(agent)} starting")
    ssh = Ssh(public_ip(agent), ssh_user(agent), ssh_options(agent))
    ssh.exec("mkdir -p hazelcast-simulator/driver-lib/")
    dest = f"hazelcast-simulator/driver-lib/maven-{version}"
    ssh.exec(f"rm -fr {dest}")
    ssh.exec(f"mkdir -p {dest}")
    for artifact_id in artifact_ids:
        ssh.scp_to_remote(f"{local_jar_path(artifact_id, version)}", f"{dest}")

    print(f"[INFO]     {public_ip(agent)} done")


def local_repo():
    cmd = "mvn org.apache.maven.plugins:maven-help-plugin:3.2.0:evaluate -Dexpression=settings.localRepository -q -DforceStdout"
    return subprocess.check_output(cmd, shell=True, text=True)


def local_jar_path(artifact_id, version):
    return f"{local_repo}/com/hazelcast/{artifact_id}/{version}/{artifact_id}-{version}.jar"


def download(artifact_id, version, repo):
    artifact = f"com.hazelcast:{artifact_id}:{version}"
    cmd = f"mvn org.apache.maven.plugins:maven-dependency-plugin:3.2.0:get -DremoteRepositories={repo} -Dartifact={artifact}"
    print(f"[INFO]{cmd}")
    exitcode = shell(cmd)
    if exitcode != 0:
        print(f"[ERROR] Failed to download artifact {artifact}")
        raise Exception(f"Failed to download artifact {artifact}")

    path = local_jar_path(artifact_id, version)
    if not os.path.exists(path):
        print(f"[ERROR] Could not find {path} in maven repo.")
        raise Exception(f"Failed to download artifact {artifact}")


def artifact_ids(enterprise, version):
    if version.startswith("3.") or version.startswith("4."):
        if enterprise:
            return ['hazelcast-enterprise-all']
        else:
            return ['hazelcast-all']
    elif version.startswith("5"):
        if enterprise:
            return ['hazelcast-enterprise', 'hazelcast-sql', 'hazelcast-spring']
        else:
            return ['hazelcast', 'hazelcast-sql', 'hazelcast-spring']
    else:
        print(f"[ERROR] Unrecognized version {version}")


def remote_repo(enteprise):
    if not enteprise:
        if version.endswith("-SNAPSHOT"):
            return "https://oss.sonatype.org/content/repositories/snapshots"
        else:
            return "https://oss.sonatype.org/content/repositories/releases"
    else:
        if version.endswith("-SNAPSHOT"):
            return "https://repository.hazelcast.com/snapshot"
        else:
            return "https://repository.hazelcast.com/release"


def is_enterprise(driver):
    if driver in ['hazelcast3', 'hazelcast4', 'hazelcast5']:
        return False
    elif driver in ['hazelcast-enterprise3', 'hazelcast-enterprise4', 'hazelcast-enterprise5']:
        return True
    else:
        print(f"Unknown driver {driver}")
        exit(1)


def parse_version_spec(version_spec):
    result = {}
    for s in version_spec.split(';'):
        index = s.index('=')
        if not index:
            print(f"Invalid version spec [{version_spec}], item [{s}] doesn't have format key=value")
        result[s[0:index].strip()] = s[index + 1:].strip()
    return result


def version(version_spec):
    parsed_version_spec = parse_version_spec(version_spec)

    version = parsed_version_spec.get('maven')
    if not version:
        print(f"Unhandled version spec: {version_spec}")
        exit(1)

    return version_spec[6:]


def force_download(version_spec):
    parsed_version_spec = parse_version_spec(version_spec)
    print(f"[INFO]version_spec:{parsed_version_spec}")
    force_download = parsed_version_spec.get('force_download')
    if not force_download:
        return False
    return "true" == force_download.lower()


agents_yaml = yaml.safe_load(sys.argv[1])
version_spec = sys.argv[2]
driver = sys.argv[3]
enterprise = is_enterprise(driver)
version = version(version_spec)
remote_repo = remote_repo(enterprise)
force_download = force_download(version_spec)
print(f"[INFO]Force download:{force_download}")
print(f"[INFO]Uploading Hazelcast jars")

local_repo = local_repo()
artifact_ids = artifact_ids(enterprise, version)

for artifact_id in artifact_ids:
    path = local_jar_path(artifact_id, version)
    if force_download and os.path.exists(path):
        os.remove(path)

    if not os.path.exists(path):
        download(artifact_id, version, remote_repo)

for artifact_id in artifact_ids:
    print(f"[INFO]Uploading {artifact_id}")

run_parallel(__upload, [(agent, artifact_ids, version) for agent in agents_yaml])
print(f"[INFO]Uploading Hazelcast jars: done")
