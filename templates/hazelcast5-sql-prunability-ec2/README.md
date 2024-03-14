Partition pruning is a set of features directed to improve performance of (mostly small batch) 
queries are scanning the IMap. We prepared this benchmarks to answer the question : 
did we succeed and are partition-prunable queries 
- really faster than regular query ?
 - are on the same performance level as Predicate API?

-------------------------

The benchmark is composed of 3 parts (in both SQL and Predicate API variations):
- scan IMap by simple key to receive simple value;
- scan IMap by constant components of compound key to receive a simple value;
- scan IMap by randomly-chosen components of compound key to receive a simple value;

We want to note about second part of this benchmark: scan IMap by constant components of compound key.
Currently (Sep, 2023), SQL engine is not able to control to which exact member it sends the query
(and as a result, which member will become the coordinator for SQL light job.). It should be changed 
after partition-aware client support for partition pruning. 
It should be split onto two parts:
- send a query to a member which contains the data;
- send a query to a member which does not contain the data;
Right now, the result for this part of the benchmark contains fluctuations, because some queries are lucky 
and job, in fact, is local (when the query request hits data a member which contains the data) and vice versa,
some queries are unlucky and the job captures two members.

-------------------------
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