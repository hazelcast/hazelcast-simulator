# Hazelcast Simulator – Existing Cluster Template

This template is designed to run Hazelcast Simulator on an existing cluster. It allows you to define node and load generator IPs and user credentials, and run performance tests without provisioning new infrastructure.

## 🔐 SSH Access Requirement

Ensure that your SSH public key (`key.pub`) is added to the `~/.ssh/authorized_keys` file on **load generators**. This is required for passwordless SSH access.

## Modify the Environment

Edit the `inventory_plan.yaml` file to specify your cluster setup:
- `cluster_name`
- IP addresses and SSH users for `loadgenerators`
- IP addresses for `nodes`

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

cluster_name: dev

loadgenerators:
  - private_ip: 172.31.6.117
    public_ip: 34.243.39.233
    user: root

nodes:
  -  172.31.4.151
  -  172.31.5.104
  -  172.31.3.126
```