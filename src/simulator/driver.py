import importlib
import importlib.util
import os
import sys
from dataclasses import dataclass
from typing import Optional

from simulator.log import info
from simulator.util import shell, run_parallel, simulator_home


def find_driver_config_file(driver, filename):
    path_cwd_config = f"{os.getcwd()}/{filename}"
    path_driver_config = f"{simulator_home}/drivers/driver-{driver}/conf/{filename}"

    # check if it exists locally
    if os.path.exists(path_cwd_config):
        return path_cwd_config

    # check if it exists in driver dir
    if os.path.exists(path_driver_config):
        return path_driver_config

    raise Exception(
        f"Could not find a configuration file with name '{filename}', looked in [{path_cwd_config},{path_driver_config}]")


def _upload_driver(host, driver_dir):
    info(f"     {host['public_ip']}  Uploading")
    shell(
        f"""rsync --checksum -avv -L -e "ssh {host['ssh_options']}" \
            {simulator_home}/{driver_dir}/* \
            {host['ssh_user']}@{host['public_ip']}:hazelcast-simulator/{driver_dir}/""")
    info(f"     {host['public_ip']}  Uploading: done")


def upload_driver(driver, hosts):
    driver_dir = f"drivers/driver-{driver}"

    info(f"Uploading driver {driver} to {driver_dir}: starting")
    run_parallel(_upload_driver, [(host, driver_dir,) for host in hosts])
    info(f"Uploading driver {driver} to {driver_dir}: done")


def driver_install_and_configure(driver: str, test: dict, is_loadgenerator: Optional[bool], params: dict, inventory_path: str):
    install_args = DriverInstallArgs(test, is_loadgenerator, inventory_path)
    _driver_exec(driver, "install.py", install_args)
    configure_args = DriverConfigureArgs(test, is_loadgenerator, inventory_path, params)
    _driver_exec(driver, "configure.py", configure_args)


@dataclass
class DriverInstallArgs:
    test: dict
    is_loadgenerator: Optional[bool]
    inventory_path: str


@dataclass
class DriverConfigureArgs:
    test: dict
    is_loadgenerator: Optional[bool]
    inventory_path: str
    coordinator_params: dict


def _driver_exec(driver: str, module: str, *args, **kwargs):
    # Add the directory containing the module to the Python path
    driver_path = f"{simulator_home}/drivers/driver-{driver}/conf/"
    module_path = f"{driver_path}/{module}"
    function_name = "exec"

    if not os.path.exists(module_path):
        info(f"Skipping {driver}.{module}")
        return

    try:
        sys.path.append(driver_path)

        # Import the module dynamically
        spec = importlib.util.spec_from_file_location("example_module", module_path)
        if spec is None:
            raise Exception(f"Could not load module {module_path} from {driver_path}")
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)

        # Call the function from the imported module
        if hasattr(module, function_name):
            function = getattr(module, function_name)
            result = function(*args, **kwargs)
            return result
        else:
            raise AttributeError(f"Function '{function_name}' not found in module '{module_path}'")
    finally:
        # Remove the directory from the Python path to avoid cluttering the path
        if driver_path in sys.path:
            sys.path.remove(driver_path)
