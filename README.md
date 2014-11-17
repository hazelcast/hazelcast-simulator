hazelcast-stabilizer
===========================

A production simulator for stress testing Hazelcast and Hazelcast based applications in clustered environments. This
can be in a local machine, but can also be in a cloud like EC2 or Google Compute Engine. The Stabilizer makes use of
JClouds, so in theory we can roll out in any cloud.

Stabilizer includes a test suite for our own stress simulation, but you can fork this repo, and add your own. 

Commercially we offer support agreements where we will integrate your tests into our runs for new releases so that your
tests act as an Application TCK. 

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
