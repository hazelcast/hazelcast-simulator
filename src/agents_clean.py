#!/usr/bin/env python3

from simulator.log import info
from simulator.ssh import Ssh
from simulator.util import run_parallel
from simulator.hosts import public_ip, ssh_user, ssh_options

def _agent_clear(agent):
    info(f"     {public_ip(agent)} Clearing agent")
    ssh = Ssh(public_ip(agent), ssh_user(agent), ssh_options(agent))
    ssh.exec(f"rm -fr hazelcast-simulator/workers/*")


def agents_clean(agents):
    info(f"Clearing agents: starting")
    run_parallel(_agent_clear, [(agent,) for agent in agents])
    info(f"Clearing agents: done")
