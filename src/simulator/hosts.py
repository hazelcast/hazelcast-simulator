def ssh_user(host):
    ssh_user = host.get('ssh_user')
    if not ssh_user:
        return "simulator"
    return ssh_user


def ssh_options(host):
    ssh_options = host.get('ssh_options')
    if not ssh_options:
        return '-i key -o StrictHostKeyChecking=no -o ConnectTimeout=60'
    return ssh_options


def public_ip(host):
    public_ip = host.get("public_ip")
    if not public_ip:
        raise Exception(f"Could not find public_ip in {host}")
    return public_ip


def agent_index(host):
    agent_index = host.get("agent_index")
    if not agent_index:
        raise Exception(f"Could not find agent_index in {host}")
    return agent_index


