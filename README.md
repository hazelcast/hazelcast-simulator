hazelcast-stabilizer
===========================

A tool for stress testing Hazelcast

### Mail Group

Please join the mail group if you are interested in using or developing Hazelcast.

[http://groups.google.com/group/hazelcast](http://groups.google.com/group/hazelcast)

#### License

Hazelcast Stabilizer is available under the Apache 2 License.

#### Copyright

Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.

Visit [www.hazelcast.com](http://www.hazelcast.com/) for more info.


#### General structure

* Test: the functionality you want to test, e.g. a map. I looks a bit like a junit test, but it doesn't use annotations
and has a bunch of methods that one can override.

* TestSuite: this is a property file that contains the name of the Test class and the properties you want to set on that
test class instance. In most cases a testsuite contains a single test class, but you can configure multiple tests within
a single testsuite.

* Failure: an indication that something has gone wrong. E.g. the Worker crashed with an OOME, an exception occurred
while doing a map.get or the result of some test didn't give the expected answer. Failures are picked up by the Agent
and send back to the coordinator.

* Worker: a JVM responsible for running a TestSuite.

* Agent: a JVM installed on a piece of hardware. Its main responsibility is spawning, monitoring and terminating workers.

* Coordinator: a JVM that can run anywhere, e.g. on your local machine. You configure it with a list of Agents ip addresses
and you send a command like "run this testsuite with 10 worker JVM's for 2 hours".

* TestCluster: responsible for spawning/terminating EC2 instances and to install Agents on remote machines. It can be used
in combination with EC2, but it can also be used in a static setup like a local machine or the Hazelcast testcluster we
have in the office.

#### Controlling Deployment

Deploying a test on workers is as simple as:

coordinator --memberWorkerCount 4 --clientWorkerCount 8 --duration 12h  map.properties

In this case we create a 4 node Hazelcast cluster and 8 clients and all load will be generated through the clients. We run
the map.properties test for a duration of 12 hours. Also m for minutes, d for days or s for seconds can be used.

One of the suggestions is that currently the profiles are configured with X clients and Y servers.

But it could be that you only want to have servers and no clients:

coordinator --memberWorkerCount 12  --duration 12h  map.properties

Or maybe you want to have a JVM with embedded client + server but all communication goes through the client:

coordinator --mixedWorkerCount 12  --duration 12h  map.properties

Or maybe you want to run 2 member JVM's per machine:

coordinator --memberWorkerCount 24  --duration 12h  map.properties

So we can very easily play with the actual deployment.