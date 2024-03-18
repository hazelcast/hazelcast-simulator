* How to design a new driver.

There are 2 main types of drivers:

1) Drivers for the load generator.
2) Drivers for the nodes (e.g. Hazelcast member)

Some products like Hazelcast and Ignite, can use the same driver for both. And in the config you will
see something like this:
```
driver: hazelcast5
```

But for products like Redis there are separate drivers for the load generators and for the nodes. 
And in the test you will see something like:

```
loadgenerator_driver=jedis5
node_driver=redis_os
```

The node_driver takes care of handling the nodes including installation and configuration. And the loadgenerator
drivers take care of running the actual load generator applications.

* Driver design

Phases of the driver
- Install: in this phase the binaries should be downloaded and installed. In case of a load generator driver, the driver also needs to be installed on the load generator nodes.
- Configure: in this phase the configuration is done. E.g. the hazelcast.xml files are created by on the configuration or the a redis.conf is applied on remote nodes.

To hook into these phases, just create a file with the appropriate phase name like 'install.py' and place it
in the 'conf' directory of the driver. When a performance test is run, the driver will be determined based on test
and and the appropriate scripts are called if they exist (otherwise the phase is ignored).

** worker.sh

For any driver where worker processes need to be created (this is always true for loadgenerators, but can be true
for products like Hazelcast/Ignite where a worker processes starts the 'members') a 'worker.sh' needs to be created.

Along this worker.sh file, a 'parameters' file is created that will dump all provides parameters to the test and it is
typical for the worker.sh to load this parameters file.

