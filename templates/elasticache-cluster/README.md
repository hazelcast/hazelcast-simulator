To modify the environment, edit the `inventory_plan.yaml`.

To create the environment.
```
inventory apply
```

To get an overview of the available instances:
```
cat inventory.yaml
```

Install the simulator and Java on the environment
```
inventory install java
inventory install simulator
```

Modify the tests by editing the tests.yaml file.

To run the tests
```
perftest run
```

To destroy the environment.
```
inventory destroy
```