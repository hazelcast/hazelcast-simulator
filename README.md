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