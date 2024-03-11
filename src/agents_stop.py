#!/usr/bin/env python3

import yaml
import sys
from simulator.hosts import public_ip, ssh_user, ssh_options
from simulator.log import info
from simulator.util import run_parallel
from simulator.ssh import Ssh


def __agent_stop(agent):
    ssh = Ssh(public_ip(agent), ssh_user(agent), ssh_options(agent))
    ssh.exec(f"hazelcast-simulator/bin/hidden/kill_agent")


agents_yaml = yaml.safe_load(sys.argv[1])
info(f"Stopping agents")
run_parallel(__agent_stop, [(agent,) for agent in agents_yaml])
info(f"Stopping agents:done")
