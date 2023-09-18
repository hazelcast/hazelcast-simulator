You have two test suites, you can run the
- `tests-sql.yaml`
- `tests-predicates.yaml`

You just need to copy one of the yamls as `tests.yaml` and you are good to go.

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
