To modify the environment, edit the `inventory_plan.yaml`.

To create the environment.
```shell
inventory apply
```

To get an overview of the available instances:
```shell
cat inventory.yaml
```

Install the simulator and Java on the environment
```shell
inventory install java
inventory install simulator
```

If you want to get the best performance for your environment
```shell
inventory tune
```

Build hazelcast binaries:

```shell
./mvnw clean install -DskipTests -T4 -Dcheckstyle.skip -Pquick -Denforcer.skip -pl hazelcast,hazelcast-sql,hazelcast-spring -am
```

Modify the tests by editing the `tests-jet.yaml` file.

To run the tests
```shell
perftest run tests-jet.yaml
```

To destroy the environment.
```shell
inventory destroy
```