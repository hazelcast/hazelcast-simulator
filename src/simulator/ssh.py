import os
import subprocess
import time

from selectors import EVENT_READ, DefaultSelector
from simulator.util import shell, exit_with_error
from simulator.log import Level, log_host


def new_key(key_path="key"):
    if os.path.exists(key_path):
        exit_with_error(f'Key [{key_path}] already exists')

    if shell(f"""ssh-keygen -P "" -m PEM -f {key_path}""") != 0:
        exit_with_error(f'Failed to generate new key')
    shell(f"chmod 400 {key_path}")


# Copied
class Ssh:

    def __init__(self,
                 ip,
                 user,
                 options,
                 silent_seconds=30,
                 use_control_socket=True,
                 log_enabled=False):
        self.ip = ip
        self.user = user
        self.options = options
        self.silent_seconds = silent_seconds
        self.log_enabled = log_enabled
        self.__connected = False
        if use_control_socket:
            self.control_socket_file = f"/tmp/{self.user}@{self.ip}.socket"
        else:
            self.control_socket_file = None

    def connect(self, check=True):
        if self.__connected:
            return

        args = f"-o ConnectTimeout=1 -o ConnectionAttempts=1 {self.options}"
        if self.control_socket_file:
            if os.path.exists(self.control_socket_file):
                self.__connected = True
                return 0
            args = f"{args} -M -S {self.control_socket_file} -o ControlPersist=5m"
        cmd = f'ssh {args} {self.user}@{self.ip} exit'
        # print(f"[INFO]{cmd}")
        exitcode = None
        max_attempts = 300
        for attempt in range(1, max_attempts):
            if attempt <= self.silent_seconds:
                exitcode = subprocess.call(cmd, shell=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            else:
                if self.log_enabled:
                    log_host(self.ip, f'Trying to connect, attempt [{attempt}/{max_attempts}], command [{cmd}]')
                result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
                log_host(self.ip, result.stdout, level=Level.info)
                log_host(self.ip, result.stderr, level=Level.warn)
                exitcode = result.returncode

            if exitcode == 0 or exitcode == 1:  # todo: we need to deal better with exit code
                self.__connected = True
                return 0
            time.sleep(1)

        if check:
            raise Exception(f"Failed to connect to {self.ip}, exitcode={exitcode}")
        else:
            return exitcode

    def __is_connected(self):
        return self.control_socket_file and os.path.exists(self.control_socket_file)

    def exec(self, command, silent=False, fail_on_error=True):
        self.connect()

        cmd_list = ["ssh"]
        if self.__is_connected():
            cmd_list.append("-S")
            cmd_list.append(f"{self.control_socket_file}")
        cmd_list.extend(self.options.split())
        cmd_list.append(f"{self.user}@{self.ip}")
        cmd_list.append(command)

        if silent:
            process = subprocess.Popen(cmd_list, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            return process.wait()

        if self.log_enabled:
            log_host(self.ip, cmd_list)

        process = subprocess.Popen(cmd_list, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        sel = DefaultSelector()
        sel.register(process.stdout, EVENT_READ)
        sel.register(process.stderr, EVENT_READ)
        while True:
            for key, _ in sel.select():
                data = key.fileobj.read1().decode()
                if not data:
                    exitcode = process.wait()

                    if exitcode != 0:
                        if fail_on_error:
                            raise Exception(f"Failed to execute [{cmd_list}], exitcode={exitcode}")
                        else:
                            return exitcode
                    return exitcode
                log_host(self.ip, data,  Level.info if key.fileobj is process.stdout else Level.warn)

    def scp_from_remote(self, src, dst_dir):
        os.makedirs(dst_dir, exist_ok=True)
        cmd = f'scp {self.options} -r -q {self.user}@{self.ip}:{src} {dst_dir}'
        self.__scp(cmd)

    def scp_to_remote(self, src, dst):
        cmd = f'scp {self.options} -r -q {src} {self.user}@{self.ip}:{dst}'
        self.__scp(cmd)

    def rsync_to_remote(self, src, dst, args='-P --checksum'):
        cmd = f'rsync {args} -e "ssh {self.options}" {src} {self.user}@{self.ip}:{dst}'
        self.__rsync(cmd)

    def __scp(self, cmd):
        self.connect()
        exitcode = subprocess.call(cmd, shell=True)
        # raise Exception(f"Failed to execute {cmd} after {self.max_attempts} attempts")

    def __rsync(self, cmd):
        self.connect()
        exitcode = subprocess.call(cmd, shell=True)
        # raise Exception(f"Failed to execute {cmd} after {self.max_attempts} attempts")
