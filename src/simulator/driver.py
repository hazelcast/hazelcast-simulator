import importlib
import importlib.util
import os
import sys
from simulator.util import shell, run_parallel, simulator_home

def find_driver_config_file(driver, filename):
    p = filename
    if os.path.exists(filename):
        return p

    p = f"{simulator_home}/drivers/{driver}/conf/{filename}"
    if os.path.exists(p):
        return p

    raise Exception(f"Could not find a configuration file with name '{filename}'")


def _upload_driver(host, driver_dir):
    print(f"[INFO]     {host['public_ip']}  Uploading")
    shell(
        f"""rsync --checksum -avv -L -e "ssh {host['ssh_options']}" {simulator_home}/{driver_dir}/* {host['ssh_user']}@{host['public_ip']}:hazelcast-simulator/{driver_dir}/""")
    print(f"[INFO]     {host['public_ip']}  Uploading: done")


def upload_driver(driver, hosts):
    driver_dir = f"drivers/driver-{driver}"

    print(f"[INFO]Uploading driver {driver} to {driver_dir}: starting")
    run_parallel(_upload_driver, [(host, driver_dir,) for host in hosts])
    print(f"[INFO]Uploading driver {driver} to {driver_dir}: done")


def driver_run(driver:str, test:str, is_server:bool, params:dict, inventory_path:str):
    install_args = DriverInstallArgs(test, is_server, inventory_path)
    _perform_function_on_driver(
        driver, "install.py", "exec", install_args)
    configure_args = DriverConfigureArgs(test, is_server, inventory_path, params)
    _perform_function_on_driver(
        driver, "configure.py", "exec", configure_args)


class DriverInstallArgs:
    def __init__(self, test, is_server, inventory_path):
        self.test = test
        self.is_server = is_server
        self.inventory_path = inventory_path

class DriverConfigureArgs:
    def __init__(self, test, is_server, inventory_path, coordinator_params):
        self.test = test
        self.is_server = is_server
        self.inventory_path = inventory_path
        self.coordinator_params = coordinator_params

def _perform_function_on_driver(driver, module, function_name, *args, **kwargs):
    # Add the directory containing the module to the Python path
    driver_path = f"{simulator_home}/drivers/driver-{driver}/conf/"
    module_path = f"{driver_path}/{module}"
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