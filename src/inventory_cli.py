#!/usr/bin/env python3
import os
import sys
import argparse
from os import path

from simulator.inventory_terraform import terraform_import, terraform_destroy, terraform_apply
from simulator.util import load_yaml_file, exit_with_error, simulator_home, shell, now_seconds
from simulator.log import info, log_header
from simulator.ssh import new_key

inventory_plan_path = 'inventory_plan.yaml'

usage = '''inventory <command> [<args>]

The available commands are:
    apply               Applies the plan and updates the inventory
    destroy             Destroy the resources in the inventory
    import              Imports the inventory from the terraform plan
    install             Installs software on the inventory
    new_key             Creates a new public/private keypair
    shell               Executes a shell command on the inventory
    tune                Tunes the environment
    inject_latencies    Injects latencies between inventory nodes
'''

# for git like arg parsing:
# https://chase-seibert.github.io/blog/2014/03/21/python-multilevel-argparse.html


default_url = "https://download.java.net/java/GA/jdk21/fd2272bbf8e04c3dbaee13770090416c/35/GPL/openjdk-21_linux-x64_bin.tar.gz"

examples = """
Oracle JDK:

        https://www.oracle.com/java/technologies/javase-jdk15-downloads.html

        When selecting the url from the download page, you need to click the link and wait till you get a popop
        and then select the url.

        Examples:
        --url=https://simulator-jdk.s3.amazonaws.com/jdk-8u261-linux-x64.tar.gz
        --url=https://download.oracle.com/otn-pub/java/jdk/15+36/779bf45e88a44cbd9ea6621d33e33db1/jdk-15_linux-x64_bin.tar.gz
        --url=https://download.oracle.com/java/17/latest/jdk-17_linux-x64_bin.tar.gz
        --url=https://download.oracle.com/java/19/latest/jdk-19_linux-x64_bin.tar.gz
        --url=https://download.oracle.com/java/20/archive/jdk-20.0.2_linux-x64_bin.tar.gz
        --url=https://download.oracle.com/java/21/latest/jdk-21_linux-x64_bin.tar.gz

OpenJDK jdk.java.net:

        https://jdk.java.net/

        Examples:
        --url=https://download.java.net/java/GA/jdk9/9.0.4/binaries/openjdk-9.0.4_windows-x64_bin.tar.gz
        --url=https://download.java.net/java/ga/jdk11/openjdk-11_linux-x64_bin.tar.gz
        --url=https://download.java.net/java/GA/jdk15/779bf45e88a44cbd9ea6621d33e33db1/36/GPL/openjdk-15_linux-x64_bin.tar.gz
        --url=https://download.java.net/java/GA/jdk17.0.2/dfd4a8d0985749f896bed50d7138ee7f/8/GPL/openjdk-17.0.2_linux-x64_bin.tar.gz
        --url=https://download.java.net/java/GA/jdk19.0.1/afdd2e245b014143b62ccb916125e3ce/10/GPL/openjdk-19.0.1_linux-aarch64_bin.tar.gz
        --url=https://download.java.net/java/GA/jdk20.0.2/6e380f22cbe7469fa75fb448bd903d8e/9/GPL/openjdk-20.0.2_linux-x64_bin.tar.gz
        --url=https://download.java.net/java/GA/jdk21/fd2272bbf8e04c3dbaee13770090416c/35/GPL/openjdk-21_linux-x64_bin.tar.gz
        --url=https://download.java.net/java/GA/jdk22.0.1/c7ec1332f7bb44aeba2eb341ae18aca4/8/GPL/openjdk-22.0.1_linux-x64_bin.tar.gz
        --url=https://download.java.net/java/GA/jdk23.0.1/c28985cbf10d4e648e4004050f8781aa/11/GPL/openjdk-23.0.1_linux-x64_bin.tar.gz

AdoptOpenJDK:

        https://github.com/AdoptOpenJDK/

        Examples of OpenJDK:
        --url=https://github.com/AdoptOpenJDK/openjdk8-binaries/releases/download/jdk8u265-b01_openj9-0.21.0/OpenJDK8U-jdk_x64_linux_openj9_8u265b01_openj9-0.21.0.tar.gz
        --url=https://github.com/AdoptOpenJDK/openjdk11-binaries/releases/download/jdk-11.0.8%2B10/OpenJDK11U-jdk_x64_linux_hotspot_11.0.8_10.tar.gz
        --url=https://github.com/AdoptOpenJDK/openjdk15-binaries/releases/download/jdk15u-2020-09-17-08-34/OpenJDK15U-jdk_x64_linux_hotspot_2020-09-17-08-34.tar.gz

        Examples of OpenJDK + J9:
        --url=https://github.com/AdoptOpenJDK/openjdk8-binaries/releases/download/jdk8u265-b01_openj9-0.21.0/OpenJDK8U-jdk_x64_linux_openj9_8u265b01_openj9-0.21.0.tar.gz
        --url=https://github.com/AdoptOpenJDK/openjdk11-binaries/releases/download/jdk-11.0.8%2B10_openj9-0.21.0/OpenJDK11U-jdk_x64_linux_openj9_11.0.8_10_openj9-0.21.0.tar.gz

Graal:

        https://github.com/graalvm/graalvm-ce-builds/

        Examples:
        --url=https://github.com/graalvm/graalvm-ce-builds/releases/download/vm-20.2.0/graalvm-ce-java8-linux-amd64-20.2.0.tar.gz
        --url=https://github.com/graalvm/graalvm-ce-builds/releases/download/vm-20.2.0/graalvm-ce-java11-linux-amd64-20.2.0.tar.gz
        --url=https://github.com/graalvm/graalvm-ce-builds/releases/download/vm-22.0.0.2/graalvm-ce-java17-linux-amd64-22.0.0.2.tar.gz

Amazon Coretto:

        https://aws.amazon.com/corretto/

        Examples:
        --url=https://corretto.aws/downloads/latest/amazon-corretto-8-x64-linux-jdk.tar.gz
        --url=https://corretto.aws/downloads/latest/amazon-corretto-11-x64-linux-jdk.tar.gz
        --url=https://corretto.aws/downloads/latest/amazon-corretto-15-x64-linux-jdk.tar.gz
        --url=https://corretto.aws/downloads/latest/amazon-corretto-16-x64-linux-jdk.tar.gz
        --url=https://corretto.aws/downloads/latest/amazon-corretto-17-x64-linux-jdk.tar.gz
        --url=https://corretto.aws/downloads/latest/amazon-corretto-19-x64-linux-jdk.tar.gz
        --url=https://corretto.aws/downloads/latest/amazon-corretto-21-x64-linux-jdk.tar.gz

Zulu:

        https://cdn.azul.com/zulu/bin/

        Examples:
        --url=https://cdn.azul.com/zulu/bin/zulu8.48.0.53-ca-jdk8.0.265-linux_x64.tar.gz
        --url=https://cdn.azul.com/zulu/bin/zulu11.41.23-ca-jdk11.0.8-linux_x64.tar.gz
        --url=https://cdn.azul.com/zulu/bin/zulu15.27.17-ca-jdk15.0.0-linux_x64.tar.gz
        --url=https://cdn.azul.com/zulu/bin/zulu17.28.13-ca-jdk17.0.0-linux_x64.tar.gz
        --url=https://cdn.azul.com/zulu/bin/zulu21.28.85-ca-jdk21.0.0-linux_x64.tar.gz

Bellsoft:

        https://bell-sw.com/pages/downloads

        Examples:
        --url=https://download.bell-sw.com/java/11.0.8+10/bellsoft-jdk11.0.8+10-linux-amd64.tar.gz
        --url=https://download.bell-sw.com/java/17.0.2+9/bellsoft-jdk17.0.2+9-linux-amd64.tar.gz

Microsoft

        https://docs.microsoft.com/en-us/java/openjdk/download

        Examples:
        --url=https://aka.ms/download-jdk/microsoft-jdk-11.0.14.1_1-31205-linux-x64.tar.gz
        --url=https://aka.ms/download-jdk/microsoft-jdk-17.0.2.8.1-linux-x64.tar.gz
"""


class InventoryInstallCli:

    def __init__(self, argv):
        usage = '''install <command> [<args>]

        The available commands are:
            java            Installs Java
            simulator       Installs Simulator
            perf            Installs Linux Perf
            async_profiler  Installs Async Profiler
            iperf3          Installs iperf3 Profiler
            tls_keystores   Installs TLS keystore and truststores for secure connections
        '''

        parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter,
                                         description='Installs software', usage=usage)
        parser.add_argument('command', help='Subcommand to run')

        args = parser.parse_args(sys.argv[2:3])
        if not hasattr(self, args.command):
            print('Unrecognized command', parser.print_help())
            exit(1)

        getattr(self, args.command)(sys.argv[3:])

    def java(self, argv):
        parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter,
                                         description='Install Java')
        parser.add_argument("--url", help="The url of the JDK tar.gz file", default=default_url)
        parser.add_argument("--examples", help="Shows example urls", action='store_true')
        parser.add_argument("--hosts", help="The target hosts.", default="all:!mc:!load_balancers")

        args = parser.parse_args(argv)

        if args.examples:
            print(examples)
            return

        hosts = args.hosts
        url = args.url

        log_header("Installing Java")
        info(f"url={url}")
        info(f"hosts={hosts}")
        cmd = f"ansible-playbook --limit {hosts} --inventory inventory.yaml {simulator_home}/playbooks/install_java.yaml -e jdk_url='{url}'"
        info(cmd)
        exitcode = shell(cmd)
        if exitcode != 0:
            exit_with_error(f'Failed to install Java, exitcode={exitcode} command=[{cmd}])')
        log_header("Installing Java: Done")

    def async_profiler(self, argv):
        parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter,
                                         description='Install Async Profiler')
        parser.add_argument("--url",
                            help="The url to the async profiler binary",
                            default="https://github.com/async-profiler/async-profiler/releases/download/v3.0/async-profiler-3.0-linux-x64.tar.gz")
        parser.add_argument("--hosts",
                            help="The target hosts.",
                            default="all:!mc")

        args = parser.parse_args(argv)

        hosts = args.hosts
        async_profiler_url = args.url
        log_header("Installing Async Profiler")
        info(f"hosts={hosts}")
        info(f"async_profiler_url={async_profiler_url}")
        cmd = f"ansible-playbook --limit {hosts} --inventory inventory.yaml {simulator_home}/playbooks/install_async_profiler.yaml -e async_profiler_url='{async_profiler_url}'"
        info(cmd)
        exitcode = shell(cmd)
        if exitcode != 0:
            exit_with_error(f'Failed to install Perf, exitcode={exitcode} command=[{cmd}])')
        log_header("Installing Async Profiler: Done")

    def perf(self, argv):
        parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter,
                                         description='Install Linux Perf')
        parser.add_argument("--hosts", help="The target hosts.", default="all:!mc:!load_balancers")

        args = parser.parse_args(argv)

        hosts = args.hosts
        log_header("Installing Perf")
        cmd = f"ansible-playbook --limit {hosts} --inventory inventory.yaml {simulator_home}/playbooks/install_perf.yaml"
        info(cmd)
        exitcode = shell(cmd)
        if exitcode != 0:
            exit_with_error(f'Failed to install Perf, exitcode={exitcode} command=[{cmd}])')
        log_header("Installing Perf: Done")

    def simulator(self, argv):
        parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter,
                                         description='Install Simulator')
        parser.add_argument("--hosts", help="The target hosts.", default="all:!mc:!load_balancers")
        args = parser.parse_args(argv)

        hosts = args.hosts

        log_header("Installing Simulator")
        info(f"hosts={hosts}")
        cmd = f"ansible-playbook --limit {hosts} --inventory inventory.yaml {simulator_home}/playbooks/install_simulator.yaml -e simulator_home='{simulator_home}'"
        info(cmd)
        exitcode = shell(cmd)
        if exitcode != 0:
            exit_with_error(f'Failed to install Simulator, exitcode={exitcode} command=[{cmd}])')
        log_header("Installing Simulator: Done")

    def iperf3(self, argv):
        parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter,
                                         description='Install iperf3')
        parser.add_argument("--hosts", help="The target hosts.", default="all:!mc:!load_balancers")
        args = parser.parse_args(argv)

        hosts = args.hosts

        log_header("Installing iperf3")
        info(f"hosts={hosts}")
        cmd = f"ansible-playbook --limit {hosts} --inventory inventory.yaml {simulator_home}/playbooks/install_iperf3.yaml -e simulator_home='{simulator_home}'"
        info(cmd)
        exitcode = shell(cmd)
        if exitcode != 0:
            exit_with_error(f'Failed to install iperf3, exitcode={exitcode} command=[{cmd}])')
        log_header("Installing iperf3: Done")

    def tls_keystores(self, argv):
        parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter,
                                         description='Install TLS keystore and truststore')
        parser.add_argument("--rsa-key-size", help="The key size to use when generating the key pair", default = "2048")
        args = parser.parse_args(argv)
        rsa_key_size = args.rsa_key_size

        log_header("Generating and installing TLS keystore and truststores")
        cmd = f"ansible-playbook --inventory inventory.yaml {simulator_home}/playbooks/install_tls_keystores.yaml -e rsa_key_size='{rsa_key_size}'"
        self._run_installation(cmd)

    def _run_installation(self, cmd):
        info(cmd)
        exitcode = shell(cmd)
        if exitcode != 0:
            exit_with_error(f'Installation failed, exitcode={exitcode} command=[{cmd}])')
        log_header("Installation complete")

class InventoryImportCli:

    def __init__(self, argv):
        parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter,
                                         description='Imports the inventory from a terraform installation.')
        parser.parse_args(argv)

        log_header("Inventory import")

        inventory_plan = load_yaml_file(inventory_plan_path)
        provisioner = inventory_plan['provisioner']

        if provisioner == "static":
            exit_with_error("Can't import inventory on static environment")
        elif provisioner == "terraform":
            terraform_plan = inventory_plan["terraform_plan"]
            terraform_import(terraform_plan)
        else:
            exit_with_error(f"Unrecognized provisioner [{provisioner}]")

        log_header("Inventory import: Done")


class InventoryApplyCli:

    def __init__(self, argv):
        parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter,
                                         description='Creates the inventory')
        parser.add_argument("-f", "--force",
                            help="Forces the destruction of the inventory (even when inventory.yaml doesn't exist)",
                            action='store_true')
        args = parser.parse_args(argv)

        log_header("Inventory apply")

        inventory_plan = load_yaml_file(inventory_plan_path)
        provisioner = inventory_plan['provisioner']
        force = args.force

        start = now_seconds()
        if not path.exists("key.pub"):
            new_key()

        if provisioner == "static":
            info(f"Ignoring create on static environment")
            return
        elif provisioner == "terraform":
            terraform_apply(inventory_plan, force)
        else:
            exit_with_error(f"Unrecognized provisioner [{provisioner}]")

        log_header("Inventory apply: Done")
        duration = now_seconds() - start
        info(f"Duration: {duration}s")


class InventoryDestroyCli:

    def __init__(self, argv):
        parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter,
                                         description='Destroys the inventory')
        args = parser.parse_args(argv)

        log_header("Inventory destroy")

        # if not force and not path.exists("inventory.yaml"):
        #    info("Ignoring destroy because inventory.yaml does not exit.")
        #    return

        inventory_plan = load_yaml_file(inventory_plan_path)
        provisioner = inventory_plan['provisioner']
        start = now_seconds()

        if provisioner == "static":
            info(f"Ignoring destroy on static environment")
            return
        elif provisioner == "terraform":
            terraform_destroy(inventory_plan, force=True)
        else:
            exit_with_error(f"Unrecognized provisioner [{provisioner}]")

        log_header("Inventory destroy: Done")
        duration = now_seconds() - start
        info(f"Duration: {duration}s")


class InventoryShellCli:

    def __init__(self, argv):
        parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter,
                                         description='Executes a shell command on the inventory', )
        parser.add_argument("command", help="The command to execute", nargs='?')
        parser.add_argument("--hosts", help="The target hosts.", default="all")
        parser.add_argument('-p', "--ping", help="Checks if the inventory is reachable", action='store_true')

        args = parser.parse_args(argv)

        hosts = args.hosts

        if args.ping:
            self.remote_ping(hosts)
        else:
            if not args.command:
                exit_with_error("Command is mandatory")
            self.remote_shell(args.command, hosts)

    def remote_ping(self, hosts):
        log_header("Inventory Ping")
        cmd = f"""ansible-playbook --limit {hosts} --inventory inventory.yaml \
                      {simulator_home}/playbooks/shell.yaml -e "cmd='exit 0'" """
        info(cmd)
        exitcode = shell(cmd)
        if exitcode != 0:
            exit_with_error(f'Inventory Ping: Failed. Command [{cmd}], exitcode={exitcode}.)')

        log_header("Inventory Ping: Done")

    def remote_shell(self, shell_cmd, hosts):
        log_header("Inventory Remote Shell")
        info(f"cmd: {shell_cmd}")
        cmd = f"""ansible-playbook --limit {hosts} --inventory inventory.yaml \
                        {simulator_home}/playbooks/shell.yaml -e "cmd='{shell_cmd}'" """
        info(cmd)
        exitcode = shell(cmd)
        if exitcode != 0:
            exit_with_error(f'Remote command failed, command [{cmd}], exitcode={exitcode}.)')

        log_header("Inventory Remote Shell: Done")


class TuneCli:

    def __init__(self, argv):
        parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter,
                                         description='Tunes the environment by configuring the set kernel parameters, '
                                                     'governor etc')
        parser.add_argument("environment", help="The type of environment to tune for", nargs='?', default="hazelcast")
        parser.add_argument("--hosts", help="The target hosts.", default="all")

        args = parser.parse_args(argv)
        environment = args.environment
        hosts = args.hosts

        playbook = f"{simulator_home}/playbooks/tune_for_{environment}.yaml"
        if not os.path.isfile(playbook):
            exit_with_error(f"Environment {environment} does not exist, could not find [{playbook}]")

        log_header("Tune for Hazelcast")
        cmd = f"""ansible-playbook --limit {hosts} --inventory inventory.yaml \
                      {simulator_home}/playbooks/tune_for_{environment}.yaml"""
        info(cmd)
        exitcode = shell(cmd)
        if exitcode != 0:
            exit_with_error(f'Tune: Failed. Command [{cmd}], exitcode={exitcode}.)')

        log_header("Tune for Hazelcast: Done")


class InventoryNewKeyCli:

    def __init__(self, argv):
        parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter,
                                         description='Creates a new public/private keypair')
        parser.add_argument("name", help="The name of the key", nargs=1)
        args = parser.parse_args(argv)

        new_key(args.name[0])


class InventoryInjectLatenciesCli:

    def __init__(self, argv):
        parser = argparse.ArgumentParser(
            formatter_class=argparse.ArgumentDefaultsHelpFormatter,
            description='Injects latencies between inventory nodes'
        )
        parser.add_argument("--latency", type=int, help="The target latency to apply (in ms)")
        parser.add_argument("--interface", default="eth0", help="Network interface to apply latency on (default: eth0)")
        parser.add_argument("--rtt", action='store_true', help="Apply latency as round-trip time (halved for one-way)")
        parser.add_argument("--profiles", help="Path to the YAML file with advanced latency profiles")

        args = parser.parse_args(argv)

        self.latency = args.latency  # Renamed from target_latency to latency
        self.network_interface = args.interface
        self.rtt = args.rtt
        self.profiles = args.profiles


        # Construct the base command
        cmd = f"ansible-playbook --inventory inventory.yaml {simulator_home}/playbooks/inject_latencies.yaml"

        # Add variables to the command
        cmd += f" -e interface='{self.network_interface}'"
        if self.latency is not None:  # Changed condition to allow 0 latency
            cmd += f" -e latency={self.latency}"
        if self.rtt:
            cmd += " -e rtt=true"
        if self.profiles:
            cmd += f" -e latency_profiles_file='{self.profiles}'"

        info(f"Running command: {cmd}")
        exitcode = shell(cmd)
        if exitcode != 0:
            exit_with_error(f"Failed to inject latencies, exitcode={exitcode}, command=[{cmd}])")

        log_header("Injecting Latencies: Done")


class InventoryClearLatenciesCli:

    def __init__(self, argv):
        parser = argparse.ArgumentParser(
            formatter_class=argparse.ArgumentDefaultsHelpFormatter,
            description='Clears latencies between inventory nodes'
        )
        parser.add_argument("--interface", default="eth0", help="Network interface to clear latency on (default: eth0)")

        args = parser.parse_args(argv)

        self.network_interface = args.interface

        cmd = f"ansible-playbook --inventory inventory.yaml {simulator_home}/playbooks/clear_latencies.yaml"

        # Add variables to the command
        cmd += f" -e interface='{self.network_interface}'"

        # Execute the command
        info(f"Running command: {cmd}")
        exitcode = shell(cmd)
        if exitcode != 0:
            exit_with_error(f"Failed to clear latencies, exitcode={exitcode}, command=[{cmd}])")

        log_header("Clearing Latencies: Done")

class InventoryCli:

    def __init__(self):
        parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter,
                                         description='Manages the inventory of resources',
                                         usage=usage)
        parser.add_argument('command', help='Subcommand to run')

        args = parser.parse_args(sys.argv[1:2])

        if not hasattr(self, args.command) and args.command != "import":
            print('Unrecognized command', parser.print_help())
            exit(1)

        if args.command == "import":
            self.import_inventory(sys.argv[2:])
        else:
            getattr(self, args.command)(sys.argv[2:])

    def import_inventory(self, argv):
        InventoryImportCli(argv)

    def apply(self, argv):
        InventoryApplyCli(argv)

    def shell(self, argv):
        InventoryShellCli(argv)

    def destroy(self, argv):
        InventoryDestroyCli(argv)

    def install(self, argv):
        InventoryInstallCli(argv)

    def new_key(self, argv):
        InventoryNewKeyCli(argv)

    def tune(self, argv):
        TuneCli(argv)

    def inject_latencies(self, argv):
        InventoryInjectLatenciesCli(argv)

    def clear_latencies(self, argv):
        InventoryClearLatenciesCli(argv)


if __name__ == '__main__':
    InventoryCli()
