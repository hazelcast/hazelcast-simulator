#!/usr/bin/env python3

from simulator.log import info
from simulator.util import shell, run_parallel
from simulator.hosts import public_ip, ssh_user, ssh_options


def _agent_download(agent, run_path, run_id):
    info(f"     {public_ip(agent)} Download")

    if run_id == "*":
        dst_path = f"hazelcast-simulator/workers/"
    else:
        dst_path = f"hazelcast-simulator/workers/{run_id}/"

    # copy the files
    # we exclude the uploads directory because it could be very big e.g jars
    shell(
        f"""rsync --copy-links -avvz --compress-level=9 -e "ssh {ssh_options(agent)}" --exclude 'upload' {ssh_user(agent)}@{public_ip(agent)}:{dst_path} {run_path}""")

    info(f"     {public_ip(agent)} Download completed")


def agents_download(agents, run_path: str, run_id: str):
    info(f"Downloading: starting")
    run_parallel(_agent_download, [(agent, run_path, run_id,) for agent in agents])
    info(f"Downloading: done")
