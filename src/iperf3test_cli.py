import argparse
import sys
import threading
from time import sleep

from inventory import load_inventory, find_host_with_public_ip
from simulator.ssh import Ssh
from simulator.util import exit_with_error


usage = '''iperf3test <command> [<args>]

The available commands are:
    pps         Creates a new performance test based on a template.
    bandwidth   Clones an existing performance test.
'''


class IPerf3TestCli:

    def __init__(self):
        parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter,
                                         description='Tests network performance between two machines.', usage=usage)
        parser.add_argument("command", help="Subcommand to run")
        args = parser.parse_args(sys.argv[1:2])

        if not hasattr(self, args.command):
            print("Unrecognized subcommand", parser.print_help())
            exit(1)

        getattr(self, args.command)()

    def pps(self):
        parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter,
                                         description='Tests max packet count between two machines.',
                                         usage='''iperf3test pps <ip1> <ip2>''')
        parser.add_argument("client", help="The client IP address.")
        parser.add_argument("server", help="The server IP address.")
        args = parser.parse_args(sys.argv[2:])

        test = IPerf3Test(args.server, args.client)
        test.run_packet_count_test()

    def bandwidth(self):
        parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter,
                                         description='Tests max bandwidth between two machines.',
                                         usage='''iperf3test bandwidth <ip1> <ip2>''')
        parser.add_argument("client", help="The client IP address.")
        parser.add_argument("server", help="The server IP address.")
        args = parser.parse_args(sys.argv[2:])

        test = IPerf3Test(args.server, args.client)
        test.run_bandwidth_test()


class IPerf3Test:

    def __init__(self, server_public_ip, client_public_ip):
        inventory = load_inventory()

        self.client = find_host_with_public_ip(inventory, client_public_ip)
        if self.client is None:
            exit_with_error(f"Could not find client [{client_public_ip} in the inventory]")

        self.server = find_host_with_public_ip(inventory, server_public_ip)
        if self.server is None:
            exit_with_error(f"Could not find server [{server_public_ip} in the inventory]")

    def run_bandwidth_test(self):
        client_args = "--parallel 1 --time 20"
        server_args = ""

        server_thread = threading.Thread(target=start_server, args=(self.server, server_args))
        server_thread.start()

        client_thread = threading.Thread(target=start_client, args=(self.client, self.server, client_args))
        client_thread.start()

    def run_packet_count_test(self):
        run_duration = 20
        client_args = f"--parallel 128 --time {run_duration} --set-mss 89"
        server_args = ""

        print(f"Testing RX PPS for {self.server['public_ip']} < {self.client['public_ip']}")

        server_thread = threading.Thread(target=start_server, args=(self.server, server_args, True))
        server_thread.start()

        client_thread = threading.Thread(target=start_client, args=(self.client, self.server, client_args, True))
        client_thread.start()

        server_pps_thread = threading.Thread(target=start_pps, args=(self.server, run_duration))
        server_pps_thread.start()

        server_thread.join()
        client_thread.join()
        server_pps_thread.join()

        print(f"Testing TX PPS for {self.server['public_ip']} > {self.client['public_ip']}")

        server_thread = threading.Thread(target=start_server, args=(self.client, server_args, True))
        server_thread.start()

        client_thread = threading.Thread(target=start_client, args=(self.server, self.client, client_args, True))
        client_thread.start()

        server_pps_thread = threading.Thread(target=start_pps, args=(self.server, run_duration))
        server_pps_thread.start()


def start_client(client, server, args, silent=False):
    sleep(2)
    ssh_client = Ssh(
        client["public_ip"],
        client["ssh_user"],
        client["ssh_options"],
        log_enabled=False,
        use_control_socket=False)
    ssh_client.connect()
    server_ip = server["private_ip"]
    iperf_cmd = f"iperf3 -c {server_ip} -p 3000 {args} --connect-timeout 30000"
    print(iperf_cmd)
    ssh_client.exec(iperf_cmd, silent)


def start_server(server, args, silent=False):
    ssh_server = Ssh(
        server["public_ip"],
        server["ssh_user"],
        server["ssh_options"],
        silent_seconds=0,
        log_enabled=False,
        use_control_socket=False)
    ssh_server.connect()
    iperf_cmd = f"iperf3 --one-off -s -p 3000 --pidfile iperf3.pid {args}"
    print(iperf_cmd)
    ssh_server.exec(f"""
        set -e
        if [ -e "iperf3.pid" ]; then
            (kill -9 $(cat iperf3.pid)) >/dev/null 2>&1
            rm iperf3.pid
        fi
        {iperf_cmd}
        echo iperf3 server done
        """, silent)


def start_pps(server, time):
    ssh_server = Ssh(
        server["public_ip"],
        server["ssh_user"],
        server["ssh_options"],
        log_enabled=False,
        use_control_socket=False)
    ssh_server.connect()
    cmd = f"hazelcast-simulator/bin/pps ens5 {time}"
    ssh_server.exec(cmd)


if __name__ == '__main__':
    IPerf3TestCli()
