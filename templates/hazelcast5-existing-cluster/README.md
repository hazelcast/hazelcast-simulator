# Hazelcast Simulator – Existing Cluster Template

This template is designed to run Hazelcast Simulator on an existing cluster. It allows you to define node and load generator IPs and user credentials, and run performance tests without provisioning new infrastructure.

**Note:** Only key-based SSH login is supported for connecting to existing hosts. Password authentication is not supported.

## 🔐 SSH Access Requirement

Ensure that your SSH public key (`key.pub`) is added to the `~/.ssh/authorized_keys` file on **all nodes and load generators**. This is required for passwordless SSH access.

## Modify the Environment

Edit the `inventory_plan.yaml` file to specify your cluster setup:
- `cluster_name`
- IP addresses and SSH users for `nodes` and `loadgenerators`

## Apply the Inventory

To generate the environment configuration:
```bash
inventory apply
```

## View the Environment
To review the generated inventory and verify the available instances:

```bash
cat inventory.yaml
```

## Install Dependencies
Install Java and the Hazelcast Simulator on the configured instances:

```bash
inventory install java
inventory install simulator
```

## Tune the Environment
To optimize the environment for performance:

```bash
inventory tune
```
## Configure and Run Tests
Modify the tests by editing the tests.yaml file.

To run the performance tests:

```bash
perftest run
```

## To clean up the inventory:

```bash
inventory destroy
```

## Example inventory_plan.yaml

```yaml
provisioner: existing-cluster

cluster_name: test-cluster

nodes:
  - private_ip: 10.0.0.10
    public_ip: 10.0.0.10
    user: root

  - private_ip: 10.0.0.11
    public_ip: 10.0.0.11
    user: root

  - private_ip: 10.0.0.12
    public_ip: 10.0.0.12
    user: root

loadgenerators:
  - private_ip: 10.0.0.21
    public_ip: 10.0.0.21
    user: root

  - private_ip: 10.0.0.22
    public_ip: 10.0.0.22
    user: root
```