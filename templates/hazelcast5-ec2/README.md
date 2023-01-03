To modify the environment, edit the 'inventory_plan.yaml'.

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

If you want to get the best performance for your environment
```
inventory tune
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