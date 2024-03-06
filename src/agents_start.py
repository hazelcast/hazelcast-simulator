#!/usr/bin/env python3

import yaml
import sys

from simulator.log import info
from simulator.util import run_parallel
from simulator.hosts import public_ip, ssh_user, ssh_options
from simulator.ssh import Ssh


def __start_agent(agent):
    info(f"     {public_ip(agent)} starting")
    ssh = Ssh(public_ip(agent), ssh_user(agent), ssh_options(agent))
    agent_start = "hazelcast-simulator/bin/hidden/agent_start"
    ssh.exec(f"{agent_start} {agent['agent_index']} {public_ip(agent)} {agent['agent_port']}")


agents_yaml = yaml.safe_load(sys.argv[1])
info(f"Starting agents")
run_parallel(__start_agent, [(agent,) for agent in agents_yaml])
info(f"Starting agents: done")
