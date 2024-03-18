* How to design a new driver.

There are 2 main types of drivers:

1) Drivers for the load generator.
2) Drivers for the nodes (e.g. Hazelcast member). This is  passive driver since it will not generate load.

Some products like Hazelcast and Ignite, can use the same driver for both. And in the config you will
see something like this:
```yaml
driver: hazelcast5
```

But for products like Redis there are separate drivers for the load generators and for the nodes. 
And in the test you will see something like:

```yaml
loadgenerator_driver=jedis5
node_driver=redis_os
```

The node_driver takes care of handling the nodes including installation and configuration. And the loadgenerator
drivers take care of running the actual load generator applications.

This makes it possible to change the front end driver from e.g  lettuce to jedis or the backend driver from redis-os
to redis-ee independently.

* Driver design

Phases of the driver
- Install: in this phase the binaries should be downloaded and installed. In case of a load generator driver, the driver also needs to be installed on the load generator nodes.
- Configure: in this phase the configuration is done. E.g. the hazelcast.xml files are created by on the configuration or the a redis.conf is applied on remote nodes.

To hook into these phases, just create a file with the appropriate phase name like 'install.py' and place it
in the 'conf' directory of the driver. When a performance test is run, the driver will be determined based on test
and the appropriate scripts are called if they exist (otherwise the phase is ignored).

Each of the functions on a driver, will get the appropriate arguments. E.g. the install function, will get a DriverInstallArgs
and configure will get a DriverConfigureArgs. These arguments contain all the relevant information including the 
test definition.

The coordinator_params field on the DriverConfigureArgs can be used to add parameters on the fly to a test. For
more information see the parameters section.

** worker.sh

For any driver where worker processes need to be created (this is always true for loadgenerators, but can be true
for products like Hazelcast/Ignite where a worker processes starts the 'members') a 'worker.sh' needs to be created.

Along this worker.sh file, a 'parameters' file is created that will dump all provides parameters to the test and it is
typical for the worker.sh to load this parameters file.

** parameters

Every test has a set of parameters like 'name', 'driver' or 'duration'. Arbitrary parameters can be added to a test 
and these parameters are available within the driver scripts and available in the Java driver. So if you want to have
a new parameter 'foobar' with value '10', just add

```yaml
    foobar: 10
```

Do not use parameters which is purely made from capitals. These are reserved for parameters which get injected transparently and it
could cause conflicts.

Unused parameters are ignored by the system. So you will not get any feedback if there is a typo in a parameter name.

** Java Driver
Currently all the load generator require a Java driver since currently only Java based load generators are supported. 
Some drivers use the same driver for the load generator as for the node (member); but the driver for the member will
be passive (so will not produce load). A good example of such a driver is the Hazelcast4PlusDriver.

* Templates

The make the simulator easy to use, there should be templates. A template is a self contained test project that is already
configured with a fully working configuration (often using a complete EC2 setup). The only thing a user needs to do
is to pick the right template when creating a test project and start changing the settings.

Simulator isn't bound to EC2; currently we only have EC2 based templates. But it should not be difficult to add
Azure or GCP based templates. This doesn't require any change in the Simulator apart from adding the appropriate 
templates. Simulator uses Terraform; so it is just a matter of creating the appropriate Terraform configuration.

To make the template easy to use, it is recommended to have a 'setup' script that takes care of provisioning and 
configuring the environnment (including the installation of Simulator, Java etc), instead of manually calling

```bash
inventory apply
inventory install java
inventory install simulator
```

** Static environments

In some cases you want to test on an environment that doesn't need to be provisioned, e.g. a lab. In this case the
terraform scripts can be deleted and the only thing that needs to be added is an 'inventory.yaml' file containing 
the host information. The simplest way to determine the format, it generate a test project using the default template and
call 'inventory apply'. This will generate a 'inventory.yaml' which can be used as the basis.