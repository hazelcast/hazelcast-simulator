# Hazelcast Simulator – Existing Cluster Template

This template is designed to run Hazelcast Simulator on an existing cluster. It allows you to run performance tests without provisioning new infrastructure.

## 🔐 SSH Access Requirement

Ensure that your SSH public key (`key.pub`) is added to the `~/.ssh/authorized_keys` file on **load generators**. This is required for passwordless SSH access.

## Modify the Environment

Edit the `inventory.yaml` file to specify your loadgenerator setup:
- IP addresses and SSH users for `loadgenerators`

Edit the `client-hazelcast.xml` file to specify your hazelcast cluster:
- Add hazelcast member addresses under the cluster-members section
- Edit cluster name to your hazelcast cluster name

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

## Example inventory_plan.yaml

```yaml
loadgenerators:
  hosts:
    34.243.39.233:
      ansible_ssh_private_key_file: key
      ansible_user: root
      private_ip: 172.31.6.117
```

## Example client-hazelcast.xml

```xml
<?xml version="1.0" encoding="UTF-8"?>
<hazelcast-client
        xsi:schemaLocation="http://www.hazelcast.com/schema/client-config
            http://www.hazelcast.com/schema/config/hazelcast-client-config-5.0.xsd"
        xmlns="http://www.hazelcast.com/schema/client-config"
        xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">

    <cluster-name>dev</cluster-name>

    <network>
        <cluster-members>
            <address>172.31.4.151:5701</address>
            <address>172.31.5.104:5701</address>
            <address>172.31.3.126:5701</address>
        </cluster-members>
    </network>

    <properties>
        <property name="hazelcast.logging.type">log4j2</property>
    </properties>

</hazelcast-client>

```