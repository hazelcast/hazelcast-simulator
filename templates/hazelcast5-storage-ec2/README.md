The purpose of this simulator test is to test Hazelcast tiered-storage.

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

Modify the tests by editing the `tests.yaml` file.

To run the tests
```shell
perftest run
```

To destroy the environment.
```shell
inventory destroy
```