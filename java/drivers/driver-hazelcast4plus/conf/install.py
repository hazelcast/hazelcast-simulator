import os.path
import subprocess
from simulator.driver import upload_driver, DriverInstallArgs
from inventory import load_hosts
from simulator.util import run_parallel, shell
from simulator.ssh import Ssh


def _upload(host, artifact_ids, version, driver):
    print(f"[INFO]     {host['public_ip']} starting")

    ssh = Ssh(host['public_ip'], host['ssh_user'], host['ssh_options'])
    ssh.exec("mkdir -p hazelcast-simulator/driver-lib/" + driver + "/")
    dest = f"hazelcast-simulator/driver-lib/{driver}/maven-{version}"
    ssh.exec(f"mkdir -p {dest}")
    for artifact_id in artifact_ids:
        ssh.rsync_to_remote(f"{_get_local_jar_path(artifact_id, version)}", f"{dest}")

    print(f"[INFO]     {host['public_ip']} done")


def _get_local_repo():
    cmd = ("mvn org.apache.maven.plugins:maven-help-plugin:3.2.0:evaluate "
           "-Dexpression=settings.localRepository -q -DforceStdout")
    return subprocess.check_output(cmd, shell=True, text=True)


def _get_local_jar_path(artifact_id, version):
    return f"{_get_local_repo()}/com/hazelcast/{artifact_id}/{version}/{artifact_id}-{version}.jar"


def _download_from_maven_repo(artifact_id:str, version:str, repo:str):
    artifact = f"com.hazelcast:{artifact_id}:{version}"
    cmd = (f"mvn org.apache.maven.plugins:maven-dependency-plugin:3.2.0:get "
           f"-DremoteRepositories={repo} -Dartifact={artifact}")
    print(f"[INFO] {cmd}")
    exitcode = shell(cmd)
    if exitcode != 0:
        print(f"[ERROR] Failed to download artifact {artifact}")
        raise Exception(f"Failed to download artifact {artifact}")

    path = _get_local_jar_path(artifact_id, version)
    if not os.path.exists(path):
        print(f"[ERROR] Could not find {path} in maven repo.")
        raise Exception(f"Failed to download artifact {artifact}")


def _get_artifact_ids(enterprise:bool, version:str):
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


def _get_remote_repo(is_enterprise:bool, version:str):
    if not is_enterprise:
        if version.endswith("-SNAPSHOT"):
            return "https://oss.sonatype.org/content/repositories/snapshots"
        else:
            return "https://oss.sonatype.org/content/repositories/releases"
    else:
        if version.endswith("-SNAPSHOT"):
            return "https://repository.hazelcast.com/snapshot"
        else:
            return "https://repository.hazelcast.com/release"


def _get_is_enterprise(driver:str):
    if driver in ['hazelcast4', 'hazelcast5']:
        return False
    elif driver in ['hazelcast-enterprise4', 'hazelcast-enterprise5']:
        return True
    else:
        print(f"Unknown driver {driver}")
        exit(1)


def _parse_version_spec(version_spec:str):
    result = {}
    for s in version_spec.split(';'):
        index = s.index('=')
        if not index:
            print(f"Invalid version spec [{version_spec}], item [{s}] doesn't have format key=value")
        result[s[0:index].strip()] = s[index + 1:].strip()
    return result


def _get_version(version_spec:str):
    parsed_version_spec = _parse_version_spec(version_spec)

    version = parsed_version_spec.get('maven')
    if not version:
        print(f"Unhandled version spec: {version_spec}")
        exit(1)

    return version_spec[6:]


def _get_force_download_from_maven_repo(version_spec:str):
    parsed_version_spec = _parse_version_spec(version_spec)
    force_download = parsed_version_spec.get('force_download')
    if not force_download:
        return False
    return "true" == force_download.lower()


def _upload_hazelcast_jars(args:DriverInstallArgs, hosts):
    driver = _get_driver(args)
    is_enterprise = _get_is_enterprise(driver)
    version_spec = args.test.get('version', 'maven=5.4.0')
    version = _get_version(version_spec)
    remote_repo = _get_remote_repo(is_enterprise, version)
    force_download = _get_force_download_from_maven_repo(version_spec)

    print(f"[INFO] Uploading Hazelcast jars")
    artifact_ids = _get_artifact_ids(is_enterprise, version)
    for artifact_id in artifact_ids:
        path = _get_local_jar_path(artifact_id, version)
        if force_download and os.path.exists(path):
            os.remove(path)

        if not os.path.exists(path):
            _download_from_maven_repo(artifact_id, version, remote_repo)

    for artifact_id in artifact_ids:
        print(f"[INFO] Uploading {artifact_id}")

    run_parallel(_upload, [(host, artifact_ids, version, driver) for host in hosts])

    print(f"[INFO] Uploading Hazelcast jars: done")


def _get_driver(args):
    return args.test.get('driver')


def exec(args:DriverInstallArgs):
    print("[INFO] Install")

    if args.is_passive:
        host_pattern = args.test.get("node_hosts", "nodes")
    else:
        host_pattern = args.test.get("loadgenerator_hosts", "loadgenerators")

    hosts = load_hosts(inventory_path=args.inventory_path, host_pattern=host_pattern)

    upload_driver(_get_driver(args), hosts)
    _upload_hazelcast_jars(args, hosts)

    print("[INFO] Install: done")
