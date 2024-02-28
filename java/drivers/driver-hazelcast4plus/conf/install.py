#!/usr/bin/env python3

import os.path
import subprocess

import sys

from agents_upload_driver import upload_driver
from inventory import load_hosts
from simulator.util import run_parallel, shell, load_yaml_file, write_file, parse_bool
from simulator.ssh import Ssh

def __upsync(host, artifact_ids, version):
    print(f"[INFO]     {host['public_ip']} starting")

    ssh = Ssh(host['public_ip'], host['ssh_user'], host['ssh_options'])
    ssh.exec("mkdir -p hazelcast-simulator/driver-lib/" + driver + "/")
    dest = f"hazelcast-simulator/driver-lib/{driver}/maven-{version}"
    ssh.exec(f"mkdir -p {dest}")
    for artifact_id in artifact_ids:
        ssh.rsync_to_remote(f"{local_jar_path(artifact_id, version)}", f"{dest}")

    print(f"[INFO]     {host['public_ip']} done")


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


def get_artifact_ids(enterprise, version):
    if version.startswith("4."):
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


def get_remote_repo(enteprise):
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
    if driver in ['hazelcast4', 'hazelcast5']:
        return False
    elif driver in ['hazelcast-enterprise4', 'hazelcast-enterprise5']:
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


def get_version(version_spec):
    parsed_version_spec = parse_version_spec(version_spec)

    version = parsed_version_spec.get('maven')
    if not version:
        print(f"Unhandled version spec: {version_spec}")
        exit(1)

    return version_spec[6:]


def force_download_from_maven_repo(version_spec):
    parsed_version_spec = parse_version_spec(version_spec)
    print(f"[INFO]version_spec:{parsed_version_spec}")
    force_download = parsed_version_spec.get('force_download')
    if not force_download:
        return False
    return "true" == force_download.lower()


def upload_hazelcast_jars():
    global local_repo
    print(f"[INFO]Uploading Hazelcast jars")
    local_repo = local_repo()
    artifact_ids = get_artifact_ids(enterprise, version)
    for artifact_id in artifact_ids:
        path = local_jar_path(artifact_id, version)
        if force_download_from_maven_repo and os.path.exists(path):
            os.remove(path)

        if not os.path.exists(path):
            download(artifact_id, version, remote_repo)
    for artifact_id in artifact_ids:
        print(f"[INFO]Uploading {artifact_id}")
    run_parallel(__upsync, [(host, artifact_ids, version) for host in hosts])
    print(f"[INFO]Uploading Hazelcast jars: done")


def exec(test_yaml, is_server__, inventory_path):
    print("[INFO] install")

    global is_server
    is_server = is_server__

    if is_server:
        host_pattern = test_yaml.get("node_hosts")
        if host_pattern is None:
            host_pattern = "nodes"
    else:
        host_pattern = test_yaml.get("loadgenerator_hosts")
        if host_pattern is None:
            host_pattern = "loadgenerators"

    global hosts
    hosts = load_hosts(inventory_path=inventory_path, host_pattern=host_pattern)

    global driver
    driver = test_yaml.get('driver')

    global enterprise
    enterprise = is_enterprise(driver)

    # todo: this is where we could allow for server/client to run with different version
    global version_spec
    version_spec = test_yaml.get('version')

    global version
    version = get_version(version_spec)

    global remote_repo
    remote_repo = get_remote_repo(enterprise)

    global force_download
    force_download = force_download_from_maven_repo(version_spec)

    upload_driver(driver, hosts)
    upload_hazelcast_jars()
