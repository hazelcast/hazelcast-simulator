import argparse
import datetime
import getpass
import hashlib

import re
import shlex
import shutil
import random
import string
import tempfile
import uuid
from datetime import datetime
import os
from os import path

import csv

import simulator.util
from agents_clean import agents_clean
from agents_download import agents_download
from inventory import load_hosts
from simulator.driver import driver_install_and_configure
from simulator.git import get_last_commit_hash, git_init, is_git_installed, is_inside_git_repo, \
    commit_modified_files
from simulator.hosts import public_ip, ssh_user, ssh_options
from simulator.ssh import Ssh, new_key
from simulator.util import read_file, write_file, shell, run_parallel, exit_with_error, simulator_home, shell_logged, \
    remove_dir, \
    load_yaml_file, parse_tags, write_yaml, AtomicLong
from simulator.log import info, warn, log_header

default_tests_path = 'tests.yaml'
inventory_path = 'inventory.yaml'


class PerfTest:

    def __init__(self, logfile=None, log_shell_command=False, exit_on_error=False):
        self.logfile = logfile
        self.log_shell_command = log_shell_command
        self.exit_on_error = exit_on_error
        self.exit_code = None
        self.verified_hosts = set()

    def _verify_host(self, host, error_counter):
        ssh = Ssh(public_ip(host), ssh_user(host), ssh_options(host))

        exitcode = ssh.connect()
        if exitcode != 0:
            warn(f"     {public_ip(host)} Verify connection: Failed")
            error_counter.inc()
            return
        info(f"     {public_ip(host)} Verify connection: Ok")

        exitcode = ssh.exec("java -version", silent=True)
        if exitcode != 0:
            warn(f"     {public_ip(host)} Verify Java: Failed")
            error_counter.inc()
        else:
            info(f"     {public_ip(host)} Verify Java: Ok")

        exitcode = ssh.exec("cd hazelcast-simulator", silent=True)
        if exitcode != 0:
            warn(f"     {public_ip(host)} Verify simulator: Failed")
            error_counter.inc()
        else:
            info(f"     {public_ip(host)} Verify simulator: Ok")

    def verify_hosts(self, host_pattern):
        if host_pattern in self.verified_hosts:
            return

        info(f"Host verification [{host_pattern}]: starting")
        hosts = load_hosts(host_pattern=host_pattern)
        error_counter = AtomicLong()
        run_parallel(self._verify_host, [(host, error_counter) for host in hosts])

        if error_counter.get() > 0:
            exit_with_error(f"Hosts verification [{host_pattern}]: failed")

        self.verified_hosts.add(host_pattern)
        info(f"Host verification [{host_pattern}]: done")

    def __kill_java(self, host):
        ssh = Ssh(public_ip(host), ssh_user(host), ssh_options(host))
        ssh.exec("hazelcast-simulator/bin/hidden/kill_java")

    def kill_java(self, host_pattern):
        log_header(f"perftest kill_java [{host_pattern}]: started")
        hosts = load_hosts(host_pattern=host_pattern)
        run_parallel(self.__kill_java, [(host,) for host in hosts])
        log_header(f"perftest kill_java [{host_pattern}]: done")

    def run(self, tests_file, tags, skip_report, test_commit, test_pattern, run_label):
        if test_commit:
            info("Automatic test commit enabled.")
            if not is_git_installed():
                exit_with_error("git is not installed.")

            if not is_inside_git_repo():
                info("No local git repo found, creating one.")
                git_init()

            commit_modified_files(datetime.now().strftime("%m/%d/%Y, %H:%M:%S"))

        tests = load_yaml_file(tests_file)
        for test in tests:
            if test_pattern is not None:
                regex = re.compile(test_pattern)
                test_name = test.get("name")
                if test_name is None:
                    continue

                match = regex.search(test_name)
                if not match:
                    print(f"Skipping test {test_name}")
                    continue

            repetitions = test.get('repetitions', 1)
            run_path = test.get('run_path')
            if run_label is not None:
                test['run_label'] = run_label
            run_label = test.get('run_label')
            if repetitions <= 0:
                continue
            elif repetitions > 1:
                if run_path is not None:
                    exit_with_error(
                        f"Test {test['name']} can't have repetitions larger than 1 with a run_path configured.")
                elif run_label is not None:
                    exit_with_error(
                        f"Test {test['name']} can't have repetitions larger than 1 with a run_label configured.")

            for i in range(0, repetitions):
                exitcode, run_path = self.run_single_test(test)

                if exitcode == 0:
                    if not skip_report:
                        self.collect(run_path,
                                     tags,
                                     warmup_seconds=test.get('warmup_seconds'),
                                     cooldown_seconds=test.get('cooldown_seconds'))
                elif self.exit_on_error:
                    exit_with_error(f"Failed run coordinator, exitcode={self.exitcode}")
        return

    def run_single_test(self, test):
        self._sanitize_test(test)
        self.clean()
        run_path = test.get('run_path')
        if run_path is None:
            name = test['name']
            run_label = test.get('run_label')
            if not run_label:
                dt = datetime.now().strftime("%d-%m-%Y_%H-%M-%S")
                run_path = f"runs/{name}/{dt}"
            else:
                run_path = f"runs/{name}/{run_label}"
                remove_dir(run_path)
            test['run_path'] = run_path
        test['RUN_ID'] = hashlib.sha256(run_path.encode("utf-8")).hexdigest()[:16]

        coordinator_params = {}
        for key, value in test.items():
            if not key == 'test':
                coordinator_params[key] = value

        driver = test.get('driver')
        if driver is not None:
            driver_install_and_configure(driver, test, None, coordinator_params, inventory_path)
        else:
            node_driver = test.get('node_driver')
            if node_driver is not None:
                driver_install_and_configure(node_driver, test, False, coordinator_params, inventory_path)

            loadgenerator_driver = test.get('loadgenerator_driver')
            driver_install_and_configure(loadgenerator_driver, test, True, coordinator_params, inventory_path)

        test_inner = test['test']
        with tempfile.NamedTemporaryFile(mode="w", delete=False, prefix="perftest_", suffix=".txt") as tmp:
            if isinstance(test_inner, list):
                for t in test_inner:
                    if 'name' in t:
                        test_name = t['name']
                    else:
                        test_name = t['class'].split('.')[-1]
                    for key, value in t.items():
                        tmp.write(test_name + '@')
                        tmp.write(f"{key}={value}\n")
            else:
                for key, value in test_inner.items():
                    tmp.write(f"{key}={value}\n")

            tmp.flush()

            coordinator_param = ""
            for key, value in coordinator_params.items():
                coordinator_param = f"{coordinator_param} --param {key}={shlex.quote(str(value))}"

            self.exitcode = self.__shell(f"{simulator_home}/bin/hidden/coordinator {coordinator_param} {tmp.name}")
            del test['run_path']
            hosts = load_hosts(inventory_path=inventory_path, host_pattern="all:!mc")
            agents_download(hosts, run_path, test['RUN_ID'])
            agents_clean(hosts)
            return self.exitcode, run_path

    def _sanitize_test(self, test: dict):
        if test.get('name') is None:
            exit_with_error(f"Test is missing 'name' property")

        if test.get('test') is None:
            exit_with_error(f"test {test['name']} is missing a 'test' section.")

        driver = test.get('driver')
        loadgenerator_driver = test.get('loadgenerator_driver')
        node_driver = test.get('node_driver')
        if driver is not None:
            if loadgenerator_driver is not None:
                exit_with_error(
                    f"test {test['name']} can't have both the driver and loadgenerator_driver configured.")
            if node_driver is not None:
                exit_with_error(f"test {test['name']} can't have both the driver and node_driver configured.")
        else:
            if loadgenerator_driver is None:
                exit_with_error(f"test {test['name']} has no driver or loadgenerator_driver configured.")

        members = test.get('members')
        if members is not None:
            if test.get('node_count') is not None:
                exit_with_error("node_count and members can't be set at the same time.")

            warn("'members' is a deprecated property, use 'node_count' instead.")
            del test['members']
            test['node_count'] = members

        node_count = test.get("node_count")
        if node_count is None:
            test['node_count'] = 0

        clients = test.get('clients')
        if clients is not None:
            if test.get('loadgenerator_count') is not None:
                exit_with_error("loadgenerator_count and clients can't be set at the same time.")

            warn("'clients' is a deprecated property, use 'loadgenerator_count' instead.")
            del test['clients']
            test['loadgenerator_count'] = clients

        loadgenerator_count = test.get('loadgenerator_count')
        if loadgenerator_count is None:
            test['loadgenerator_count'] = -1

        node_hosts = test.get('node_hosts', 'all')
        if not node_hosts:
            node_hosts = "all|!mc"
            test['node_hosts'] = node_hosts
        self.verify_hosts(node_hosts)

        loadgenerator_hosts = test.get('loadgenerator_hosts')
        if not loadgenerator_hosts:
            loadgenerator_hosts = "all|!mc"
            test['loadgenerator_hosts'] = loadgenerator_hosts
        self.verify_hosts(loadgenerator_hosts)

        if test.get("duration") is None:
            exit_with_error(f"The 'duration' property is not specified in test {test.get('name')}")

        if test.get("parallel") is None:
            test['parallel'] = False

        if test.get("performance_monitor_interval_seconds") is None:
            test['performance_monitor_interval_seconds'] = 1

        if test.get("fail_fast") is None:
            test['fail_fast'] = True

        if test.get("verify_enabled") is None:
            test['verify_enabled'] = True

    def clean(self):
        # the !mc pattern is very ugly
        hosts = load_hosts(inventory_path=inventory_path, host_pattern="all:!mc")
        agents_clean(hosts)

    def __shell(self, cmd):
        if self.log_shell_command:
            info(cmd)

        if self.logfile:
            return shell_logged(cmd, self.logfile, exit_on_error=False)
        else:
            return shell(cmd, use_print=True)

    def collect(self, dir, tags, warmup_seconds=None, cooldown_seconds=None):
        report_dir = f"{dir}/report"

        if not warmup_seconds:
            warmup_seconds = 0

        if not cooldown_seconds:
            cooldown_seconds = 0

        if not os.path.exists(report_dir):
            self.__shell(f"perftest report  -w {warmup_seconds} -c {cooldown_seconds} -o {report_dir} {dir}")

        if is_inside_git_repo():
            test_commit_hash = get_last_commit_hash()
            if test_commit_hash is not None:
                commit_file = f"{dir}/test_commit"
                simulator.util.write_file(commit_file, test_commit_hash)

        csv_path = f"{report_dir}/report.csv"
        if not os.path.exists(csv_path):
            warn(f"Could not find [{csv_path}]")
            return

        run_id_path = f"{dir}/run.id"
        if not os.path.exists(run_id_path):
            write_file(run_id_path, uuid.uuid4().hex)

        tags['run_id'] = read_file(run_id_path)

        results = {}
        with open(csv_path, newline='') as csv_file:
            csv_reader = csv.reader(csv_file, delimiter=',', quotechar='|')
            # skip header
            next(csv_reader)
            for row in csv_reader:
                measurements = {
                    '10%(us)': row[2],
                    '20%(us)': row[3],
                    '50%(us)': row[4],
                    '75%(us)': row[5],
                    '90%(us)': row[6],
                    '95%(us)': row[7],
                    '99%(us)': row[8],
                    '99.9%(us)': row[9],
                    '99.99%(us)': row[10],
                    'max(us)': row[11],
                    'operations': row[12],
                    'duration(ms)': row[13],
                    'throughput': row[14]}
                results[row[1]] = {'tags': tags, 'measurements': measurements}

        write_yaml(f"{dir}/results.yaml", results)


class PerftestCreateCli:

    def __init__(self, argv):
        parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter,
                                         description="Creates a new performance test based on a template")
        parser.add_argument("name",
                            help="The name of the performance test.", nargs='?')
        parser.add_argument("-t", "--template",
                            help="The name of the performance test template.", default="hazelcast5-ec2")
        parser.add_argument("--id",
                            help="An extra id to make resources unique. By default the username is used.")
        parser.add_argument("--list", help="List available performance test templates", action='store_true')
        args = parser.parse_args(argv)

        if args.list:
            for template in os.listdir(f"{simulator_home}/templates"):
                info(template)
            return

        name = args.name
        template = args.template

        if not args.name:
            exit_with_error("Can't create performance test, name is missing")

        log_header(f"Creating performance test [{name}]")
        info(f"Using template: {template}")
        cwd = os.getcwd()
        target_dir = os.path.join(cwd, name)

        if os.path.exists(target_dir):
            exit_with_error(f"Can't create performance [{target_dir}], the file/directory already exists")

        self.__copy_template(target_dir, template)

        os.chdir(target_dir)
        new_key()
        id = args.id if args.id else getpass.getuser()
        self.__process_templates(target_dir, id)
        os.chdir(cwd)

        log_header(f"Creating performance test [{name}]: Done")

    def __copy_template(self, target_dir, template):
        templates_dir = f"{simulator_home}/templates"
        template_dir = os.path.join(templates_dir, template)
        if not os.path.exists(template_dir):
            exit_with_error(f"Template directory [{template_dir}] does not exist.")

        shutil.copytree(template_dir, target_dir)

    def __process_templates(self, target_dir, id):
        for subdir, dirs, files in os.walk(target_dir):
            for filename in files:
                filepath = subdir + os.sep + filename
                if os.access(filepath, os.W_OK):
                    new_text = read_file(filepath).replace("<id>", id)
                    rnd = ''.join(random.choices(string.ascii_lowercase, k=5))
                    new_text = new_text.replace("<rnd:5>", rnd)
                    write_file(filepath, new_text)


class PerftestCloneCli:

    def __init__(self, argv):
        parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter,
                                         description="Clones an existing performance test")
        parser.add_argument("source", help="The name of source performance test.")
        parser.add_argument("destination", help="The directory for the destination performance test.")
        parser.add_argument("--force", help="Force using the destination directory if it already exists",
                            action='store_true')

        log_header("Cloning Performance test")
        args = parser.parse_args(argv)
        src = args.source
        dest = args.destination
        self.__check_valid_perftest(src)

        if args.force:
            info(f"Removing [{dest}].")
            remove_dir(dest)
        else:
            if path.exists(dest):
                exit_with_error(f"Can't copy performance test to [{dest}], the directory already exists.")

        self.__clone(src, dest)
        self.__add_parent(src, dest)

        log_header(f"Performance test  [{src}] successfully cloned to [{dest}]")

    def __check_valid_perftest(self, src):
        if not path.exists(src):
            exit_with_error(f"Directory [{src}] does not exist.")

        if not path.exists(f"{src}/inventory_plan.yaml"):
            exit_with_error(f"Directory [{src}] is not a valid performance test.")

    def __clone(self, src, dest):
        shutil.copytree(src, dest)

        remove_dir(f"{dest}/runs")
        remove_dir(f"{dest}/logs")
        remove_dir(f"{dest}/venv")
        remove_dir(f"{dest}/.idea")
        remove_dir(f"{dest}/.git")
        remove_dir(f"{dest}/key")
        remove_dir(f"{dest}/key.pub")
        remove_dir(f"{dest}/.gitignore")

        # get rid of the terraform created files.
        for subdir, dirs, files in os.walk(dest):
            for dir in dirs:
                if dir.startswith(".terraform"):
                    remove_dir(subdir + os.sep + dir)
            for file in files:
                if file.startswith("terraform.tfstate") or file.startswith(".terraform"):
                    remove_dir(subdir + os.sep + file)

    def __add_parent(self, src, dest):
        with open(f"{dest}/clone_parent.txt", "w") as file:
            file.write("# Name of the parent performance test this performance test is cloned from.\n")
            file.write(src)


class PerftestRunCli:

    def __init__(self, argv):
        parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter,
                                         description='Runs a tests.yaml which is a self contained set of tests')
        parser.add_argument('file', nargs='?', help='The tests file', default=default_tests_path)

        parser.add_argument('-c', '--commit',
                            action='store_true',
                            help="Automatically commits all modified files to the Git repo and add "
                                 "the hash of the last commit to the run directory. "
                                 "This way the exact configuration for a benchmark can always be restored. "
                                 "A Git repo will automatically be created if one doesn't exist.")

        parser.add_argument('-k', '--kill_java', nargs=1, default=[True], type=bool,
                            help='If all the Java processes should be killed before running using hosts all:!mc')

        parser.add_argument('-t', '--tag', metavar="KEY=VALUE", nargs=1, action='append')

        parser.add_argument('-p', '--pattern',
                            nargs=1,
                            default=[None],
                            help="The pattern (regex) of the tests to run within the test yaml file.")

        # parser.add_argument('--runPath',
        #                     nargs=1,
        #                     help="The path where the result of the run need to be stored.")

        parser.add_argument('-rl', '--runLabel',
                            nargs=1,
                            default=[None],
                            help="The label of the run to use as run results directory name instead of timestamp.")

        parser.add_argument('-agr', '--skipReport',
                            action='store_true', dest='skip_report', default=False,
                            help="When set, this flag stops the automatic generation of reports after test completion.")

        args = parser.parse_args(argv)
        tags = parse_tags(args.tag)
        pattern = args.pattern[0]
        # kill_java = args.kill_java
        # run_path = args.runPath
        run_label = args.runLabel[0]

        tests_file = args.file
        perftest = PerfTest()

        perftest.run(tests_file, tags, args.skip_report, args.commit, pattern, run_label)


class PerftestKillJavaCli:

    def __init__(self, argv):
        parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter,
                                         description='Kills all Java processes')
        parser.add_argument("--hosts", help="The target hosts.", default="all:!mc")
        args = parser.parse_args(argv)

        hosts = args.hosts

        perftest = PerfTest()
        perftest.kill_java(hosts)


class PerftestCollectCli:

    def __init__(self, argv):
        parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter,
                                         description='Collects the results from a performance test')
        parser.add_argument("dir", help="The directory with the test runs")
        parser.add_argument('-t', '--tag', metavar="KEY=VALUE", nargs=1, action='append')
        parser.add_argument('-w', '--warmup', nargs=1, default=[0], type=int,
                            help='The warmup period in seconds. The warmup removes datapoints from the start.')
        parser.add_argument('-c', '--cooldown', nargs=1, default=[0], type=int,
                            help='The cooldown period in seconds. The cooldown removes datapoints from the end.')

        args = parser.parse_args(argv)

        tags = parse_tags(args.tag)

        log_header("perftest collect")
        perftest = PerfTest()
        perftest.collect(args.dir, tags, warmup_seconds=args.warmup, cooldown_seconds=args.cooldown)

        log_header("perftest collect: done")


class PerftestCleanCli:

    def __init__(self, argv):
        parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter,
                                         description='Cleans all the files generated by workers on the agents.')
        args = parser.parse_args(argv)

        log_header("perftest clean")

        perftest = PerfTest()
        perftest.clean()

        log_header("perftest clean: done")
