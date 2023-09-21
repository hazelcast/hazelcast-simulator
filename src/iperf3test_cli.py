import argparse
import threading
from time import sleep

from inventory import load_inventory, find_host_with_public_ip
from simulator.ssh import Ssh
from simulator.util import exit_with_error


class IPerf3Test:

    def __init__(self):
        parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter,
                                         description='Does a bandwidth test between 2 machines.')
        parser.add_argument("client", help="The client IP address.")
        parser.add_argument("server", help="The server IP address.")
        args = parser.parse_args()

        inventory = load_inventory()

        client_args = "--parallel 1  -t 10"
        server_args = ""

        client_public_ip = args.client
        client = find_host_with_public_ip(inventory, client_public_ip)
        if client is None:
            exit_with_error(f"Could not find client [{client_public_ip} in the inventory]")

        server_public_ip = args.server
        server = find_host_with_public_ip(inventory, server_public_ip)
        if client is None:
            exit_with_error(f"Could not find server [{server_public_ip} in the inventory]")

        server_thread = threading.Thread(target=start_server, args=(server,server_args))
        server_thread.start()

        client_thread = threading.Thread(target=start_client, args=(client, server, client_args))
        client_thread.start()


def start_client(client, server, args):
    sleep(2)
    ssh_client = Ssh(
        client["public_ip"],
        client["ssh_user"],
        client["ssh_options"],
        log_enabled=False,
        use_control_socket=False)
    ssh_client.connect()
    server_ip = server["private_ip"]
    iperf_cmd = f"iperf3 -c {server_ip} -p 5701 {args} --connect-timeout 30000"
    print(iperf_cmd)
    ssh_client.exec(iperf_cmd)


def start_server(server, args):
    ssh_server = Ssh(
        server["public_ip"],
        server["ssh_user"],
        server["ssh_options"],
        silent_seconds=0,
        log_enabled=False,
        use_control_socket=False)
    ssh_server.connect()
    iperf_cmd = f"iperf3 --one-off -s -p 5701 --pidfile iperf3.pid {args}"
    print(iperf_cmd)
    ssh_server.exec(f"""
        set -e
        if [ -e "iperf3.pid" ]; then
            (kill -9 $(cat iperf3.pid)) >/dev/null 2>&1
            rm iperf3.pid
        fi
        {iperf_cmd}
        echo iperf3 server done
        """)


if __name__ == '__main__':
    IPerf3Test()
