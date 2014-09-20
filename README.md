hazelcast-stabilizer
===========================

A tool for stress testing Hazelcast and Hazelcast based applications in clustered environments. This can be in a local
machine, but can also be in a cloud like EC2 or Google Compute Engine. The Stabilizer makes use of JClouds, so in theory
we can roll out in any cloud.

We have our own tests for our own stress simulation, but you can fork this repo, and maintain your own stress tests (say proprietary code). If you want, we can run those too for you in our infra, so that we can ensure that ongoing development of Hazelcast, we will be able to meet what you might consider as an Application TCK, that we will honor. Subject to agreement, of course!

* [Hazelcast Structure](docs/STRUCTURE.md)

* [Installing Stabilizer](docs/INSTALL.md)

* [Using the Stabilizer Archetype](docs/ARCHETYPE.md)

* [Provisioning machines](docs/PROVISIONER.md)

* [Running tests with the Coordinator](docs/COORDINATOR.md)

* [Writing tests](docs/TESTS.md)

### Mail Group

Please join the mail group if you are interested in using or developing Hazelcast.

[http://groups.google.com/group/hazelcast](http://groups.google.com/group/hazelcast)

#### License

Hazelcast Stabilizer is available under the Apache 2 License.

#### Copyright

Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.

Visit [www.hazelcast.com](http://www.hazelcast.com/) for more info.
