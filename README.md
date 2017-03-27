# Table of Contents

* [Hazelcast Simulator](#hazelcast-simulator)
* [Key Concepts](#key-concepts)
* [Installing Simulator](#installing-simulator)
  * [Setting up the Local Machine](#setting-up-the-local-machine)
  * [Preparations to Setup Remote Machines](#preparations-to-setup-remote-machines)
* [Setting up for Static Setup](#setting-up-for-static-setup)
* [Setting up for Amazon EC2](#setting-up-for-amazon-ec2)
* [Setting up for Google Compute Engine](#setting-up-for-google-compute-engine)
* [Customizing your Simulator Setup](#customizing-your-simulator-setup)
  * [TestSuite](#testsuite)
  * [Simulator Properties](#simulator-properties)
  * [Preparing the Test Run](#preparing-the-test-run)
  * [Running the Test](#running-the-test)
  * [Analyzing your Simulator Run](#analyzing-your-simulator-run)
* [Provisioner](#provisioner)
  * [Creating and Destroying Instances](#creating-and-destroying-instances)
  * [Installing Hazelcast Simulator](#installing-hazelcast-simulator)
  * [Download and Clean Log Files](#download-and-clean-log-files)
  * [Stopping All Remote Processes](#stopping-all-remote-processes)
  * [Connecting to the Cloud Instances](#connecting-to-the-cloud-instances)
* [Coordinator](#coordinator)
  * [Controlling the TestSuite](#controlling-the-testsuite)
  * [Controlling the Test Duration](#controlling-the-test-duration)
  * [Controlling the Cluster Layout](#controlling-the-cluster-layout)
  * [Controlling the Load Generation](#controlling-the-load-generation)
  * [Controlling the Hazelcast Configuration](#controlling-the-hazelcast-configuration)
* [Simulator.Properties File Description](#simulator-properties-file-description)


# Hazelcast Simulator

Hazelcast Simulator is a production simulator used to test Hazelcast and Hazelcast-based applications in clustered environments. 
It also allows you to create your own tests and perform them on your Hazelcast clusters and applications that are deployed to 
cloud computing environments. In your tests, you can provide any property that can be specified on these environments (Amazon EC2, 
Google Compute Engine(GCE), or your own environment): properties such as hardware specifications, operating system, Java version, etc.

Hazelcast Simulator allows you to add potential production problems, such as real-life failures, network problems, overloaded CPU,
 and failing nodes to your tests. It also provides a benchmarking and performance testing platform by supporting performance 
 tracking and also supporting various out-of-the-box profilers.

Hazelcast Simulator makes use of Apache jclouds&reg;, an open source multi-cloud toolkit that is primarily designed for testing
 on the clouds like Amazon EC2 and GCE.

You can use Hazelcast Simulator for the following use cases:

- In your pre-production phase to simulate the expected throughput/latency of Hazelcast with your specific requirements.
- To test if Hazelcast behaves as expected when you implement a new functionality in your project.
- As part of your test suite in your deployment process.
- When you upgrade your Hazelcast version.

Hazelcast Simulator is available as a downloadable package on the Hazelcast <a href="http://www.hazelcast.org/download" target="_blank">web site</a>. 
Please refer to the [Installing Simulator section](#installing-simulator) for more information.

Simulator includes a test suite for our own stress simulation, but you can fork this repo, and add your own.

Commercially we offer support agreements where we will integrate your tests into our runs for new releases so that your
tests act as an Application TCK. 

# Key Concepts

The following are the key concepts mentioned with Hazelcast Simulator.

- **Test** - A test class for the functionality you want to test, e.g. a Hazelcast map. This test class looks similar to a JUnit 
test, but it uses custom annotations to define methods for different test phases (e.g. `@Setup`, `@Warmup`, `@Run`, `@Verify`).

- **TestSuite** - A property file that contains the name of the `Test` class and the properties you want to set on that `Test` 
class instance. A `TestSuite` contains one or multiple tests. It can also contain the same `Test` class with different names and configurations.

- **Worker** - This term `Worker` is used twice in Simulator. 

  - **Simulator Worker** - A Java Virtual Machine (JVM) responsible for running the configured `Tests`. It can be configured to 
  spawn a Hazelcast client or member instance, which is used in the tests. We refer to this `Worker` in the context of a Simulator 
  component like `Agent` and `Coordinator`.
  
  - **Test Worker** - A Runnable implementation to increase the test workload by spawning several threads in each `Test` instance. 
  We refer to this `Worker` in the context of a `Test`, e.g. how many worker threads a `Test` should create.

- **Agent** - A JVM responsible for managing client and member `Workers`. There is always one `Agent` per physical machine, no
 matter how many `Workers` are spawned on that machine. It serves as communication relay for the `Coordinator` and monitoring 
 instance for the `Workers`.

- **Coordinator** - A JVM that can run anywhere, such as on your local machine. The `Coordinator` is actually responsible for 
running the `TestSuite` using the `Agents` and `Workers`. You configure it with a list of `Agent` IP addresses, and you run it by
executing a command like "run this testsuite with 10 member worker and 100 client worker JVMs for 2 hours".

- **Coordinator Remote** - A JVM that can run anywhere, such as on your local machine. The `CoordinatorRemote` is responsible for
 sending instructions to the Coordinator. For basic simulator usages the remote is not needed, but for complex scenarios such 
 as **rolling upgrade** or **high availability** testing, a much more interactive approach is required. The coordinator remote 
 talks to the coordinator using TCP/IP.

- **Provisioner** - Spawns and terminates cloud instances, and installs Hazelcast Simulator on the remote machines. It can be used 
in combination with EC2 (or any other cloud), but it can also be used in a static setup, such as a local machine or a cluster of 
machines in your data center.

- **Failure** - An indication that something has gone wrong. Failures are picked up by the `Agent` and sent back to the `Coordinator`.

- **simulator.properties** - The configuration file you use to adapt the Hazelcast Simulator to your business needs (e.g. cloud 
provider, SSH username, Hazelcast version, Java profiler settings).

# Installing Simulator

Hazelcast Simulator needs a Unix shell to run. Ensure that your local and remote machines are running under Unix, Linux or Mac OS. 
Hazelcast Simulator may work with Windows using a Unix-like environment such as Cygwin, but that is not officially supported at the moment.

## Setting up the Local Machine

The local machine will be the one on which you will eventually execute the Coordinator to run your TestSuite. It is also the 
source to install Simulator on your remote machines.

Hazelcast Simulator is provided as a separate downloadable package, in `zip` or `tar.gz` format. You can download either one 
[here](http://hazelcast.org/download/#simulator).

After the download is completed, follow the below steps.

- Unpack the `tar.gz` or `zip` file to a folder that you prefer to be the home folder for Hazelcast Simulator. The file 
extracts with the name `hazelcast-simulator-<`*version*`>`. If your are updating Simulator you are done and can skip the 
following steps.

- Configure the environment by either one of the following steps.

  - Run the configuration wizard from the extracted folder.

    ```
    ./<extracted folder path>/bin/simulator-wizard --install
    ```  

  OR

  - Add the following lines to the file `~/.bashrc` (for Unix/Linux) or to the file `~/.profile` (for Mac OS).

    ```
    export SIMULATOR_HOME=<extracted folder path>/hazelcast-simulator-<version>
    PATH=$SIMULATOR_HOME/bin:$PATH
    ```

- Open a new terminal to make your changes in `~/.bashrc` or `~/.profile` effective. Call the Simulator Wizard with the `--help` 
option to see if your installation was successful.

  ```
  simulator-wizard --help
  ```

## First Run of Simulator

After the installation you can already use Simulator on the local machine.

- Create a working directory for your Simulator TestSuite. Use the Simulator Wizard to create an example setup for you and 
change to that directory.

  ```
  simulator-wizard --createWorkDir tests
  cd tests
  ```
  
- Execute the created `run` script to run the TestSuite.

  ```
  ./run
  ```

Congratulations, you successfully ran Simulator for the first time! Please refer to the 
[Customizing your Simulator Setup section](#customizing-your-simulator-setup) to learn how to configure your test setup.

## Preparations to setup Remote Machines

Beside the local setup, there are also static setups (fixed list of given remote machines, e.g. your local machines, a test laboratory) 
and cloud setups (e.g. Amazon EC2, Google Compute Engine). For all those remote machines, you need to configure a password free SSH
 access. You may also need to configure the firewall between your local and the remote machines.

## Firewall Settings

Please ensure that all remote machines are reachable via TCP ports 22, 9000 and 5701 to 57xx on their external network interface 
(e.g. `eth0`). The first two ports are used by Hazelcast Simulator. The other ports are used by Hazelcast itself. Ports 9001 to 
90xx are used on the loopback device on all remote machines for local communication.

![](images/Network.png)

- Port 22 is used for SSH connections to install Simulator on remote machines, to start the Agent and to download test result and 
log files. If you use any other port for SSH, you can configure Simulator to use it via the `SSH_OPTIONS` property in the `simulator.properties` file.
- Port 9000 is used for the communication between Coordinator and Agent. You can configure this port via the `AGENT_PORT` property 
in the `simulator.properties` file.
- Ports 9001 to 90xx are used for the communication between Agent and Worker. We use as many ports as Worker JVMs are spawned 
on the machine.
- Ports 5701 to 57xx are used for the Hazelcast instances to form a cluster. We use as many ports as Worker JVMs are spawned on
 the machine, since each of them will create its own Hazelcast instance.

## Creating an RSA key pair

The preferred method for password free authentication is using an RSA (Rivest, Shamir and Adleman crypto-system) public/private key
 pair. The RSA key should not require you to enter the pass-phrase manually. A key with a pass-phrase and ssh-agent-forwarding is 
 strongly recommended, but a key without a pass-phrase also works.

If you already have an RSA key pair, you willl find the files `id_rsa.pub` and `id_rsa` in your local `~/.ssh` folder. If you do 
not have RSA keys, you can generate a public/private key pair using the following command.

```
ssh-keygen -t rsa -C "your-email@example.com"
```

Press `[Enter]` for all questions. The value for the e-mail address is not relevant in this case. After you execute this command, 
you should have the files `id_rsa.pub` and `id_rsa` in your `~/.ssh` folder.


# Setting up for Static Setup

Having installed Simulator locally, this section describes how to prepare Simulator for testing a Hazelcast cluster deployed on a
 fixed list of given remote machines, e.g. your local machines or a test laboratory.

- Create a working directory for your Simulator TestSuite. Use the Simulator Wizard to create an example setup for you and change
 into the directory.

  ```
  simulator-wizard --createWorkDir tests --cloudProvider static
  cd tests
  ```

- Add the IP addresses of your remote machines to the file `agents.txt`, one address per line.

  ```
  192.0.2.0.1
  192.0.2.0.2
  ```

  You can also configure a different public and private IP address per machine (with 192.0.2.0 being the public and 172.16.16.0 
  the private IP address ranges).

  ```
  192.0.2.0.1,172.16.16.1
  192.0.2.0.2,172.16.16.2
  ```

  The public IP addresses will be used by the Provisioner and Coordinator to connect to the remote machines. The private IP 
  addresses will be used by Hazelcast to form a cluster.

- The default username used by Hazelcast Simulator is `simulator`. You can change this via the `USER` property in the `simulator.properties` file in your working folder.

  ```
  USER=preferredUserName
  ```

  Ensure that a user account with this name exists on all configured remote machines.

- Ensure you have appended your public RSA key (`id_rsa.pub`) to the `~/.ssh/authorized_keys` file on all remote machines. You 
can create and execute a script to copy the RSA key to all machines in your `agents.txt` file with the following commands.

  ```
  simulator-wizard --createSshCopyIdScript
  ./ssh-copy-id-script
  ```

- You can check if the SSH connection for all remote machines work as expected using the following command.

  ```
  simulator-wizard --sshConnectionCheck
  ```

- Execute the created `prepare` script to install Simulator on the remote machines.

  ```
  ./prepare
  ```

- Execute the created `run` script to run the TestSuite.

  ```
  ./run
  ```

Congratulations, you successfully ran Simulator on your remote machines! Please refer to the 
[Customizing your Simulator Setup section](#customizing-your-simulator-setup) to learn how to configure your test setup.


# Setting up for Amazon EC2

Having installed Simulator, this section describes how to prepare Simulator for testing a Hazelcast cluster deployed at Amazon EC2.

Simulator provides support to create and terminate EC2 instances via the [Provisioner](#provisioner). If you want to create and 
setup the EC2 instances by yourself, please use the configuration as described in [Setting up for Static Setup](#setting-up-for-static-setup).

The Provisioner uses AWS access keys (access key ID and secret access key) for authentication 
(see [Types of Security Credentials](http://docs.aws.amazon.com/general/latest/gr/aws-sec-cred-types.html)). Please 
see [Creating, Disabling, and Deleting Access Keys for your AWS Account](http://docs.aws.amazon.com/general/latest/gr/managing-aws-access-keys.html) 
to generate and download your access keys.

For security reasons we store the cloud credentials outside of the working directory (e.g. to prevent an accidental commit into 
your project files). The default locations for the credentials are

- `~/ec2.identity` for the access key ID
- `~/ec2.credential` for the secret access key

You can store the credentials in a different location, but then you need to configure the `simulator.properties` later.

- Create a working directory for your Simulator TestSuite. Use the Simulator Wizard to create an example setup for you and change 
to that directory.

  ```
  simulator-wizard --createWorkDir tests --cloudProvider aws-ec2
  cd tests
  ```

- If you stored your AWS credentials in a different location please update the paths of `CLOUD_IDENTITY` and `CLOUD_CREDENTIALS` 
in your `simulator.properties` file.

- Execute the created `prepare` script to create the EC2 instances and install Simulator on them.

  ```
  ./prepare
  ```

- Execute the created `run` script to run the TestSuite.

  ```
  ./run
  ```

- Execute the following command to destroy the created EC2 instances.

  ```
  provisioner --terminate
  ```

Congratulations, you successfully ran Simulator on Amazon EC2! Please refer to the 
[Customizing your Simulator Setup section](#customizing-your-simulator-setup) to learn how to configure your test setup.

![image](images/NoteSmall.jpg) ***NOTE***: *Creating the credential files in your home directory instead of directly setting the 
access key ID and secret access key in the `simulator.properties` file is for security reasons. It is too easy to share your
 credentials with the outside world. Now you can safely add the `simulator.properties` file in your source repository or 
 share it with other people.*

# Customizing your Simulator Setup

After you installed Hazelcast Simulator for your environment and did a first run, it is time to learn more about the setup and customize it.

## TestSuite

The TestSuite defines the Simulator Tests which are executed during the Simulator run. The generated TestSuite file is 
`test.properties`, which contains a single test.

```
IntIntMapTest@class = com.hazelcast.simulator.tests.map.IntIntMapTest
IntIntMapTest@threadCount = 10
IntIntMapTest@putProb = 0.1
```

Tests can be configured with properties to change the behavior of the test (e.g. the number of used keys or the probability of 
GET and PUT operations). With properties you can re-use the same code to test different scenarios. They are defined in the following format:

```
TestId@key = value
```

There are two special properties which are used by the Simulator framework itself.

| Property | Example value | Description |
|:-|:-|:-|
| `class` | `com.hazelcast.simulator.tests.map.IntIntMapTest` | Defines the fully qualified class name for the Simulator Test. 
Used to create the test class instance on the Simulator Worker.<br>This is the only mandatory property which has to be defined. |
| `threadCount` | `5` | Defines the number of worker threads for Simulator Tests which use the @RunWithWorker annotation. |

All other properties must match a public field in the test class. If a defined property cannot be found in the Simulator Test 
class or the value cannot be converted to the according field type, a BindException is thrown. If there is no property defined 
for a public field, its default value will be used. 

## Simulator Properties

You can configure Simulator itself using the file `simulator.properties` in your working directory. The default properties are
 always loaded from the `${SIMULATOR_HOME}/simulator-tests/simulator.properties` file. Your local properties are overriding the
  defaults. You can compare your `simulator.properties` with the default values with the following command.

```
simulator-wizard --compareSimulatorProperties
```

Often changed properties are the `INSTANCE_TYPE` to specify the instance type for cloud setups or the `VERSION_SPEC` to run Simulator
 with a different Hazelcast version.

Please refer to the [Simulator.Properties File Description section](#simulator-properties-file-description) for detailed 
information about the `simulator.properties` file.

## Preparing the Test Run

To prepare a Simulator run you have to ensure that the remote machines are available (e.g. your cloud instances have been created)
 and that Simulator has been installed on them.

- For a `local` setup there is nothing to do.

- For a `static` setup you just need to install Simulator on them. The created prepare script executes a single command to achieve this.

  ```
  provisioner --install
  ```

- For any cloud based setup you need to create the instances. The created prepare script executes a single command to achieve this.

  ```
  provisioner --scale 2
  ```
  
  This will create two instances in your configured cloud environment. Simulator will automatically be installed during the instance creation.

Please refer to the [Provisioner section](#provisioner) for detailed information about the arguments of Provisioner.

## Running the Test

The actual Simulator Test run is done by the `Coordinator`. The created `run` script is a good start to customize your test setup.
 I takes four optional parameters to define the number of member and client Workers, the run duration and the name of the TestSuite 
 file. So the following command will spawn four member Workers, twenty client Workers and will run for five minutes (with the default `test.properties` file).
 
 ```
 ./run 4 20 5m
 ```

The run script also defines a number of JVM options like verbose GC logging and Java heap sizes. It also shows how to define 
Hazelcast [System Properties](#system-properties) like the partition count. You can customize these values to tune your TestSuite execution.

Please refer to the [Coordinator section](#coordinator) for detailed information about the arguments of Coordinator.

## Analyzing your Simulator Run

During the simulator run, a directory is created that stores all output for that given run. By default this directory's name is a
 timestamp such as `2016-08-02__22_08_09`. After the test is completed, all artifacts from the remote workers are downloaded to 
 this directory. So if you have, for example, enabled `Flightrecorder`, then you find the generated JFR files there as well. 

The name of this output directory can be modified by using the `--sessionId` command line option. It is recommended to clean up 
the remote workers once in a while if they stay around for an extended period. You can clean up using the following command: 

```
coordinator --clean
```

To download all artifacts manually, execute the following command:

```
coordinator --download
```

Both `clean` and `download` commands allow you to pass `sessionId`. Please see the following examples:

```
coordinator --download 2016-08-02__22_08_09
coordinator --clean 2016-08-02__22_08_09
```


# Provisioner

The provisioner is responsible for creating and destroying cloud instances. It will create an instance of the configured type, 
open firewall ports and install Java and Hazelcast Simulator on it.

You can configure the cloud provider, operating system, region, hardware, JVM version and Hazelcast version through
the file `simulator.properties`. Please see the [Simulator.Properties File Description section](#simulator-properties-file-description) for more information. 

## Creating and destroying instances 

To create instances use `--scale` with the target size. This command will create a single instance.

```
provisioner --scale 1
```

Executing the following command will create two new instances (to a total of three).  

```
provisioner --scale 3
```

To scale back to two cloud instances execute the following command.

```
provisioner --scale 2
```

You can terminate all existing instances with these commands.   

```
provisioner --terminate
```

OR

```
provisioner --scale 0
```

The file `agents.txt` will be updated automatically by Provisioner.

## Installing Hazelcast Simulator

If you already have your cloud instances provisioned or run a `static` setup you can just install Hazelcast Simulator with the following command.

```
provisioner --install
```

This is also useful whenever you update or change your local installation and want to re-install Hazelcast Simulator on the remote 
machines. This is just necessary if the JAR files have been changed. Configuration changes in your `test.properties` or
 `simulator.properties` don't require a new Simulator installation.

## Stopping all remote processes

If your test run hangs for any reason you can kill all Java processes on the remote machines with the following command:

```
provisioner --kill
```

After that you can download the log files and analyze what went wrong.

## Connecting to the cloud instances

By default the Provisioner creates a user with the name `simulator`. That user is added to the sudoers list. Also, the public RSA 
key of your local user is copied to the remote machine and added to the file `~/.ssh/authorized_keys`. You can login to that 
machine using the following command.

```
ssh simulator@ip
```

You can change the name of the created user by setting the `USER=preferredUserName` property in the file `simulator.properties`. 
Be careful not to pick a name that is already used on the target image. For example `ec2-user` or `ubuntu` often exist and you can
 run into authentication problems if you use the same username.


# Coordinator

The Coordinator is responsible for actually running the Simulator Tests.

You can start the Coordinator without any parameters.

```
coordinator
```

This command will use default values for all mandatory parameters, e.g. the file `test.properties` as TestSuite, a single member 
Worker as cluster layout, and 60 seconds for the test duration.

## Controlling the TestSuite

You can specify the used TestSuite file by adding a single non-option argument (an argument without an `--option`).

```
coordinator small-testsuite.properties
```

## Controlling the Test Duration

You can control the duration of the test execution by using the `--duration` argument. The default duration is 60 seconds. 
You can specify the time unit for this argument by using

- `s` for seconds
- `m` for minutes
- `h` for hours
- `d` for days

If you omit the time unit the value will be parsed as seconds.

You can see the usage of the `--duration` argument in the following example commands.

```
coordinator --duration 90s
coordinator --duration 3m
coordinator --duration 12h
coordinator --duration 2d
```

The duration is used as the run phase of a Simulator Test (that's the actual test execution). If you have long running warmup or
 verify phases, the total runtime of the TestSuite will be longer.

There is another option for the use case where you want to run a Simulator Test until some event occurs (which is not time bound), 
e.g. stop after five million operations have been done. In this case, the test code must stop the `TestContext` itself. Use the 
following command to let Coordinator wait indefinitely.

```
coordinator --waitForTestCaseCompletion
```

## Controlling the Cluster Layout

Hazelcast has two basic instance types: member and client. The member instances form the cluster and client instances connect to 
an existing cluster. Hazelcast Simulator can spawn Workers for both instance types. You can configure the number of member and 
client Workers and also their distribution on the available remote machines. Available remote machines are the ones, that are 
configured in the `agents.txt` file (either manually in static setups or via Provisioner in cloud setups).

Use the options `--members` and `--clients` to control how many member and client Workers you want to have. The following command 
creates a cluster with four member Workers and eight client Workers (which connect to that cluster).

```
coordinator --members 4 --clients 8
```

A setup without client Workers is fine, but out of the box it won't work without member Workers.

The Workers will be distributed among the available remote machines with a round robin selection. By default, the machines will 
be mixed with member and client Workers. You can reserve machines for member Workers. The distribution of machines will then 
be limited to the according group of remote machines. Use the following command to specify the number of dedicated member machines:

```
coordinator --dedicatedMemberMachines 2
```

You cannot specify more dedicated member machines than you have available. If you define client Workers, there must be at least 
a single remote machine left (e.g. with three remote machines you can specify a maximum of two dedicated member machines). The 
round robin assignment will be done in the two sub-groups of remote machines.

If you need more control over the cluster layout, you can make use of the `coordinator-remote` which allows full control on 
layout, versions of clients, servers, etc. Please refer to the [Coordinator Remote section](#coordinator-remote).

## Controlling the Load Generation

Beside the cluster layout you can also control which Workers will execute their RUN phase. The default is that client Workers 
are preferred over member Workers. That means if client Workers are used, they will create the load in the cluster, otherwise 
the member Workers will be used. In addition you can limit the number of Workers which will generate the load.

```
coordinator --targetType member --targetCount 2
```

This will limit the load generation to two member Workers, regardless of the client Workers' availability. Please have a look 
at command line help via `coordinator --help` to see all allowed values for these arguments.

## Controlling the Hazelcast Configuration

By default Coordinator uses the files `${SIMULATOR_HOME}/conf/hazelcast.xml` and `${SIMULATOR_HOME}/conf/client-hazelcast.xml` 
to configure the created Hazelcast instances. You can override these files by placing a `hazelcast.xml` or `client-hazelcast.xml` 
in your working directory. 


# Coordinator Remote

The Simulator remote is a powerful addition to the Coordinator. The Coordinator takes care of a lot of things such as copying 
Hazelcast to the remote machines, starting members, clients and running tests. The problem is that the Coordinator command 
line interface is very monolithic.

To open up the Coordinator, the command `coordinator-remote` is added. To give some impressions:

```
coordinator-remote worker-start --count 2
coordinator-remote test-run --duration 2m map.properties
coordinator-remote stop
```

In the above example we first create some workers, then run the map tests for two minutes and then we stop the remote. 
The `coordinator-remote` looks very simple, but it is very flexible and allows introducing complex scenarios to be tested.

## Using the CLI Manual

Quite a lot of effort was put in setting up a comprehensive CLI manual for the `coordinator-remote`. To get an overview of 
all available commands, use the following command:

```
coordinator-remote --help
```

This will show all the available commands such as `install` or `worker-start`. You can get detailed information about a 
command by adding its name, a sample of which is shown below:

```
coordinator-remote worker-start --help
```

## Configuring the Remote

By default the coordinator opens a port 5000 and listens to this port waiting for commands. If you do not want to enable 
remote commands, set the `COORDINATOR_PORT=0` in the `simulator.properties` file.

If you want to run multiple coordinators on a single machine, you need to give each coordinator instance a different port 
so that the remotes do not communicate with the wrong ports and the coordinator does not compete on getting the port.

## Basic Usage

To use the coordinator remote, it is best to work with two terminals (or let the coordinator write to file). In the first 
terminal we start the coordinator using the following command:

```
coordinator
```

Coordinator will not do anything and listen to commands from the `coordinator-remote`.

In the second terminal we enter following commands:

```
coordinator-remote worker-start
coordinator-remote test-run --duration 10m map.properties
coordinator-remote stop
```

These commands will start a single-member cluster, execute the map test for 10 minutes and then shutdown the coordinator.


## Starting Workers

The following command starts one extra worker:


```
coordinator-remote worker-start
```

Workers can be started while a test is running, but such a worker will not participate in generating a load. So in case of the 
new worker being a member, it will become an extra member in the cluster. In such cases it is probably best to generate 
load through a client.

The command returns the list of Simulator addresses of the workers that have been created and could be stored in a variable like this:

```
workers=$(coordinator-remote worker-start)
```

## Clients

The following script demonstrates a basic usage of letting a test run using five clients:

```
coordinator-remote worker-start --count 1
coordinator-remote worker-start --workerType javaclient --count 5
coordinator-remote test-run --duration 10m map.properties
coordinator-remote stop
```

## Querying

The commands such as the following execute a behavior on one or more workers:

- `test-start`
- `test-run`
- `worker-kill`
- `worker-script`

You can specify some filters using various options with these commands and this allows a flexible selection mechanism. Please see 
the following examples:


- Example for the option `versionSpec`:
  - `member-kill --versionSpec maven=3.8` which kills one member having the given version.
  <br></br>
- Example for the option `workerType`:
  - `worker-script --workerType javaclient --command 'bash:ls'` executes the `ls` command on all Java clients.
  <br></br>
- Example for the option `agents`:
  - `test-run --agents C_A1,C_A2 map.properties` runs a test on all members that belong to Agent 1 and 2.
  <br></br>
- Example for the option `workers`:
  - `test-start --workers C_A1_W1 map.properties` starts a test on worker `C_A1_W1`. Keep in mind that the `--workers` option 
  cannot be combined with the `--agents` option.
  <br></br>
- Example for the option `workerTags`:
  - `member-kill --tags bla`

All commands apart from `worker-kill` try to execute on the maximum number of items that are allowed. Only the `worker-kill` command has been defaulted to 1. 

Filters can also be combined as shown below:

```
script-member --versionSpec maven=3.8 --agents C_A1,C_A2 --command 'bash:ls'
```

The above will return the directory listing for all workers that have `versionSpec maven=3.8` AND have agent `C_A1` or `C_A2` as parent.

The number of selected members can be limited using the option `--maxCount`.

By default the selection of the workers is very predictable, but this can sometimes be a problem, for example, when you want to 
kill random members and get them killed spread equally over all members. In such situations the option `--random` can be used as shown below:

```
member-kill --random
```

## Starting Tests

There are two test commands:

- `test-run`, runs a test and waits for completion.
- `test-start`, starts a test and returns the Simulator address of the test.

The `test-start` is the logical choice if you want to interact with the `coordinator-remote` during the execution of a test. 
Perhaps you want to kill a member while a test on the clients is running.

The following command shows a basic example of the `test-run`:

```
coordinator-remote test-run --duration 5m map.properties
```

In this case the map test is executed for five minutes.

The following command shows the basic usage of the `test-start`:

```
test_id=$(coordinator-remote test-start --duration 5m map.properties
```

In this case the map test is executed for five minutes. The call will return immediately and the ID of the test is written 
to the `test_id` Bash variable.

You can control which worker is going to execute a test.

### Target Count

You can control the number of workers that will execute a test using the option `targetCount` as shown below:

```
coordinator-remote worker-start --count 10
coordinator-remote test-run --targetCount 3 map.properties
```

Even though there are 10 members, only three are being used to generate load.

### Worker Type

Using the `worker-type` you can control what type of worker is going to act as a driver (so has `timestep-threads` running). If 
there are only members, then by default all members will be drivers. If there are one or more clients (lite member is considered 
a form of client), then only the clients will act as drivers.

```
coordinator-remote worker-start --count 2
coordinator-remote test-run --duration 1m --targetType litemember map.properties
```

In the above example, two member workers will act as drivers.

```
coordinator-remote worker-start --count 2
coordinator-remote worker-start --workerType javaclient --count 4
coordinator-remote worker-start --workerType litemember --count 8
coordinator-remote test-run --duration 1m --targetType litemember map.properties
```

In this contrived example above, eight lite members will be drivers and the Java clients will be completely ignored. The current 
available types of workers are as follows:

- `member`
- `litemember`
- `javaclient`

As soon as the native clients are added, you will be able to configure, for example C# or C++ clients.

If a non-member `workerType` is defined, then these workers will be the drivers. 

### Warmup and Duration

By default a test will not do any warmup and will run till the test is explicitly stopped. 

```
coordinator-remote test-run map.properties
```

But you can configure the warmup and duration as shown below:

```
coordinator-remote test-run --warmup 1m --duration 10m map.properties
```

Valid time units are as follows:

- s: second
- m: minute
- h: hour
- d: day

Using the query options like agents, workers and tags, you have the perfect control on which workers are going to run a particular 
test. For more information please see the [Querying section](#querying). 

## Stopping Test

A test can be stopped using the `test-stop` command. Please see the example below:

```
test_id=$(coordinator-remote test-start map.properties)
...
coordinator-remote test-stop $test_id
```

This command waits till the test stops. It will return the status of test on completion as "failed" or "completed".

## Status of a Test

If you are running a test with `test-start`, you probably want to periodically ask what the status of the test is. You can do this 
using the `test-status` command as shown below:

```
testId=$(coordinator-remote test-start map.properties)
... doing stuff
coordinator-remote test-status $testId
```

This command returns the phase of test as "running", "setup", etc. Or, it returns "completed" if the test is completed successfully 
or "failed" in case of a failure. The actual reason of the failure can be found in the `failures.txt` in the created session directory.

For a more comprehensive example see the [Rolling Upgrade Test section](#rolling-upgrade-test).

## Killing Workers

It is possible to kill one or more members while doing a test. This is useful, for example, for resilience testing. In such cases 
it is probably best to use clients as drivers. Please see the below example:

```
coordinator-remote worker-start --count 4
coordinator-remote worker-start --workerType javaclient --count 1 
test_id=$(coordinator-remote test-start map.properties)
sleep 60
coordinator-remote worker-kill 
sleep 60
coordinator-remote test-stop $test_id
coordinator-remote stop
```

In the above example we start with a four-member cluster and the client doing a map test. Then we sleep 60 seconds, we keep a 
random member and we wait for another 60 seconds. Then we stop the test and wait for completion.

The `worker-kill` is a very flexible command. You can kill a specific worker using its simulator address, create all workers on a 
given agent, or kill all workers with a given version. Please see the [Querying section](#querying) for more information.

You can fully control how a worker is going to get killed. It can be done, for example, by executing a bash script:

```
coordinator-remote worker-kill 'bash:kill -9 $PID'
```

The `$PID`, the PID of the worker, is available as an environment variable for the bash script.

You can also send an embedded JavaScript as shown below:

```
coordinator-remote worker-kill 'js:code that kills the JVM'
```

Using the JavaScript you can execute commands on the JVM without needing to have the that code on the worker. Access to the 
Hazelcast instance is possible using the injected `hazelcastInstance` environment variable. 

In theory it is possible to execute any JVM scripting language, such as Groovy, by prefixing the command by the extension of that 
language, e.g., `groovy:...`. This will cause the scripting engine to load the appropriate scripting language. But you need to 
make sure the appropriate JAR files are set on the worker's classpath.

Another way to kill a member is by causing an OOME as shown below:

```
coordinator-remote worker-kill OOME
```

The OOME is actually built on top of the JavaScript version and it allocates big arrays till the JVM runs out of memory.

By default a `System.exit(0)` is called.

## Executing Scripts on Workers

It is possible to execute scripts on the workers. This can be a bash script, an embedded JavaScript or any other embedded JVM 
scripting language. Please see the [Killing Workers](#killing-workers) for more information about the scripting options.

For example, the following command will return the directory listing for every worker:

```
coordinator-remote worker-script --command `bash:ls`
```


### Fire and Forget

By default the `worker-script` command will wait for the results of the `execute` command. But in some cases, the script should be 
executed in a fire and forget fashion. This can be done using the `--fireAndForgetFlag` option as shown below:

```
coordinator-remote worker-script --fireAndForgetFlag  --command 'bash:jstack $PID'
```

This command makes a thread dump of each worker JVM.

For more options regarding selecting the target members, see the [Querying section](#querying).

## Using Custom Hazelcast Version

With the `coordinator-remote`, it is very easy to control the exact version of Hazelcast being used. In the below example, the 
3.7.1 version is used, but using the `versionSpec 'git=hash'` one can also execute a particular commit.
            
```
version=maven=3.7.1
coordinator &
coordinator-remote install ${version}
coordinator-remote worker-start --count 1 --versionSpec ${version}
coordinator-remote test-start --duration 10m map.properties
coordinator-remote stop
```

Because the remote is interactive, you can easily combine different Hazelcast versions in the same cluster. In the below example a 
3.7 member is combined with a 3.7.1 member. This makes it an ideal tool to check patch level compatibility.

It is important to realize that the versions used in the `worker-start` need to be installed using `coordinator-remote install` before being used. 

## Combining Hazelcast Versions

Because the coordinator is now interactive, it is possible to mix different versions as shown below:

```
version_1=maven=3.7.1
version_2=maven=3.7
coordinator-remote install ${version_1}
coordinator-remote install ${version_2}
coordinator-remote worker-start --versionSpec ${version_1}
coordinator-remote worker-start --versionSpec ${version_2}
coordinator-remote test-run --duration 10m map.properties
coordinator-remote stop
```

In the example above, we create a two-member cluster with a different patch level Hazelcast version. This is ideal for patch level 
compatibility testing.

We can also combine a client of a different version than the members. This is ideal for client compatibility testing:

```
member_version=maven=3.6
client_version=maven=3.7
coordinator-remote install ${version_1}
coordinator-remote install ${version_2}
coordinator-remote worker-start --versionSpec {member_version}
coordinator-remote worker-start --workerType=javaclient --versionSpec {client_version}
coordinator-remote test-run --duration 10m map.properties
coordinator-remote stop
```

In the above example we start a one-member cluster using Hazelcast 3.6 and a Hazelcast Java client using 3.7.

## Rolling Upgrade Test

Because it is easy to mix versions and we can kill and start new members, we can easily create a rolling upgrade test. So imagine 
we want to verify if a Hazelcast 3.7 cluster can be upgraded to 3.8, we could start out with a 3.7 cluster, start a bunch of 
clients, start some tests on the clients that verify the cluster is behaving correctly and then one by one replace the 3.7 members 
by 3.8 members. Once all members have been upgraded we complete the test. This scenario is depicted in the following example:

```
members=10
clients=2
old_version=maven=3.7
new_version=maven=3.8

coordinator-remote install ${old_version}
coordinator-remote install ${new_version}
coordinator-remote worker-start --count ${members} --versionSpec ${old_version}
coordinator-remote worker-start --count ${clients} --workerType javaclient --versionSpec ${old_version}
test_id=$(coordinator-remote test-start --duration 0s atomiclong.properties)
sleep 30s

for i in {1..$members}
do
   coordinator-remote worker-kill --versionSpec ${old_version}
   coordinator-remote worker-start --versionSpec ${new_version}

   sleep 10s

   status=$(coordinator-remote test-status $test_id)
   echo test_status $status
   if [ $status == "failure" ]; then
       echo Error detected!!!!!!!
       break
   fi
done

coordinator-remote test-stop $test_id
coordinator-remote stop
```

### Resilience Testing

Script should not cause a member to die; if it does, the test will be aborted with a failure. Scripts can be used for a lot of 
purposes. One could read out some data, modify internal behavior of the Hazelcast cluster. It can for example be used to 
deliberately cause problems that should not lead to dying members. Perhaps one could close some connections, consume most CPU 
cycles, use most of the memory so that the member is almost running into an OOME.

## Tags

Tags are the last pieces of the puzzle. A tag can be given to an agent, worker or test. For example the following tags could be
 defined on agents:

```
10.31.44.31:clients
10.31.44.31:members
```

Using these tags, you can create workers:

```
coordinator-remote worker-start --agentsTags clients --workerType client
```

The created worker will be spawned on the first agent, since that is the agent with the `clients` tag. Once the worker is created, 
it will automatically inherit all tags from the agent.

Tags do not need to be a single identifier, it may contain multiple identifiers such as `foo,a=1,b=true`. Quite a few commands 
have support for filtering on tags, please see the [Querying section](#querying).

You can created workers with their own tags:

```
coordinator-remote worker-start --tags b=10 --workerType client
```

Tags are practical because they allow for a mechanism to configure, for example the `hazelcast.xml`, in a much more fine 
grained way.

## Testing WAN replication

???


# Profiling your Simulator Test

To determine, for example, where the time is spent or other resources are being used, you want to profile your application. 
The recommended way to profile is using the Java Flight Recorder (JFR) which is only available in the Oracle JVMs. The JFR, 
unlike the other commercial profilers like JProbe and Yourkit, does not make use of sampling or instrumentation. It hooks into 
some internal APIs and is quite reliable and causes very little overhead. The problem with most other profilers is that they 
distort the numbers and frequently point you in the wrong direction; especially when I/O or concurrency is involved. Most of 
the recent performance improvements in Hazelcast are based on using JFR.

To enable the JFR, the JVM settings for the member or client need to be modified depending on what needs to be profiled. 
Please see the following example:

```
JFR_ARGS="-XX:+UnlockCommercialFeatures  \
          -XX:+FlightRecorder \
          -XX:StartFlightRecording=duration=120m,filename=recording.jfr  \
          -XX:+UnlockDiagnosticVMOptions \
          -XX:+DebugNonSafepoints"

coordinator --members 1 \
            --workerVmOptions "$JFR_ARGS" \
            --clients 1 \
            --clientVmOptions "$JFR_ARGS" \
            sometest.properties
```

In the above example, both client and members are configured with JFR. Once the Simulator test has completed, all artifacts 
including the JFR files are downloaded. The JFR files can be opened using the Java Mission Control command `jmc`.

## Reducing Fluctuations

Fore more stable performance numbers, set the minimum and maximum heap size to the same value. Please see the following example:

```
coordinator --members 1 \
            --workerVmOptions "-Xmx4g -Xms4g" \
            --clients 1 \
            --clientVmOptions "-Xmx1g -Xms1g" \
            sometest.properties
```

Also set the minimum cluster size to the expected number of members using the following property:

```
-Dhazelcast.initial.min.cluster.size=4
```

This prevents Hazelcast cluster from starting before the minimum number of members has been reached. Otherwise, the benchmark 
numbers of the tests can be distorted due to partition migrations during the test. Especially with a large number of partitions 
and short tests, this can lead to a very big impact on the benchmark numbers.

## Enabling Diagnostics

Hazelcast 3.7+ has a diagnostics system which provides detailed insights on what is happening inside the client or server 
`HazelcastInstance`. It is designed to run in production and has very little performance overhead. It has so little overhead 
that we always enable it when doing benchmarks.

```
coordinator --members 1 \
            --workerVmOptions "-Dhazelcast.diagnostics.enabled=true \
                               -Dhazelcast.diagnostics.metric.level=info \
                               -Dhazelcast.diagnostics.invocation.sample.period.seconds=30 \
                               -Dhazelcast.diagnostics.pending.invocations.period.seconds=30 \
                               -Dhazelcast.diagnostics.slowoperations.period.seconds=30" \
            --clients 1 \
            --clientVmOptions "-Dhazelcast.diagnostics.enabled=true \
                               -Dhazelcast.diagnostics.metric.level=info" \
            sometest.properties
```

Using the above example, both client and server have diagnostics enabled. Both will write a diagnostics file. Once the Simulator 
run is completed and the artifacts are downloaded, the diagnostics files can be analyzed.

## Enabling Different Profilers or Other Startup Customizations

If you want to use a different profiler than JFR and you require more than simple JVM args, or you want to play with features 
like numactrl, OpenOnload, etc., you need to override the worker startup script. This is done by copying the startup script to
 the working directory. For example to modify a member worker:

```
cp $SIMULATOR_HOME/conf/worker-hazelcast-member.sh .
```

This bash script controls the startup of the member. This particular file also contains full examples for the following features:

- Yourkit
- Intel VTune
- Linux Perf
- HProf
- numactrl
- dstat
- OpenOnload

It also allows to experiment with different profilers like [`honest-profiler`](https://github.com/RichardWarburton/honest-profiler).

To upload the required artifacts, create the directory `upload` in the working directory. This upload directory will automatically
 be copied to all worker machines. It can be found in the parent directory of the worker, e.g., `../upload/someartifact`.
            

## GC analysis

By adding the following options to member/client args, the benchmark generator will do a gc comparison:
```
-Xloggc:gc.log -XX:+PrintGC -XX:+PrintGCDetails -XX:+PrintGCTimeStamps  -XX:+PrintGCDateStamps
```

# Writing a Simulator test

The main part of a Simulator test is writing the actual test. The Simulator test is heavily inspired by the JUnit testing and 
Java Microbenchmark Harness (JMH) frameworks. To demonstrate writing a test, we will start with a very basic case and 
progressively add additional features. 

For the initial test case we are going to use the `IAtomicLong`. Please see the following snippet:

```java
package example;

...

public class MyTest extends AbstractTest{
  private IAtomicLong counter;

  @Setup public void setup(){
    counter = targetInstance.getAtomicLong("c");
  }

  @TimeStep public void inc(){
    counter.incrementAndGet();
  }
}
```

The above code example shows one of the most basic tests. `AbstractTest` is used to remove duplicate code from tests; so it 
provides access to a logger, `testContext`, `targetInstance` HazelcastInstance, etc. 

A Simulator test class needs to be a public, non-abstract class with a public no-arg constructor.

Assume the property file to start the test is as follows:

```
class=example.MyTest
```

The main property that needs to be in the property file is the `class` property which needs to point to the full class name.

Just like the other annotated methods, `Timestep` methods need to be public due to the code generator and they are allowed to 
throw `Throwable` like a checked exceptions:

```
  @TimeStep public void inc() throws Exception{
    counter.incrementAndGet();
  }
```

Any `Throwable`, apart from the `StopException`, that is being thrown will lead to a Failure to be reported.

## Adding properties

Properties can be added to a test to make it easy to modify them from the outside. Properties must be public fields and can be 
primitives, wrappers around primitives like `java.lang.Long`, enums, strings and classes. Properties are case sensitive.

In the below example the `countersLength` property has been added and it defaults to 20.

```
public class MyTest extends AbstractTest{
  public int countersLength = 20;

  private IAtomicLong[] counters;

  @Setup public void setup(){
    this.counters = new IAtomicLong[countersLength];
    for(int k=0;k<countersLength;k++)
      counters[k] = targetInstance.getAtomicLong(""+k);
  }

  @TimeStep public void inc(BaseThreadState state){
      int counterIndex = state.randomInt(countersLength);
      counters[counterIndex].incrementAndGet();
  }
}
```

In most cases it is best to provide defaults for properties to make customization of a test less verbose.

The `countersLength` can be configured as shown below:

```
class=example.MyTest
countersLength=1000
```

The order of the properties in the file is irrelevant.

Properties do not need to be simple fields. The property binding supports complex object graphs to be created and configured.
Properties can be nested and no-arg constructor must be used to build up the graph of objects. Please see the following example:

```
public class SomeTest{
	
	pubic Config config;

	public static class Config{
		NestedConfig nestedConfig;
	}

	public static class NestedConfig{
		public int value;	
	}
}
```

The `config` object can be configured as shown below:

```
class=SomeTest
config.nestedConfig.value=10
```

If a property is not used in a test, the test fails during its startup. The reason is that if you would make a typing error and, 
in reality, something different is tested different from what you think is being tested, it is best to know this as soon as possible.

## Number of Threads

By default 10 threads are used to call the `Timestep` methods. But in practice you want to control the number of threads. 
This can be done using the `threadCount` property as shown below:

```
class=example.MyTest
threadCount=5
```

This property does not need to be defined on the test itself. It is one of the magic properties used by the Simulator.

## Probabilities

Most tests require different functionalities to be called. For example in the `IAtomicLong` case, you would like to do 10% writes 
and 90% reads. With the Simulator this is very easy. 

In the below example a new timestep method `get` has been added:

```
public class MyTest extends AbstractTest{
  public int countersLength; 

  private AtomicLong counter;

  @Setup public void setup(){
    this.counter = targetInstance.getAtomicLong("counter");
  }

  @TimeStep(prob=0.9) public void get(){
    counter.get();
  }  

  @TimeStep(prob=-1) public void inc(){
    counter.incrementAndGet();
  }
}
```

In this case the `get` method has a probability of 0.9 and the `inc` method has a probability of -1. The -1 indicates that this 
method will get all the remaining probabilities, so 1-0.9=0.1. Of course, you can configure the `inc` method to have a 
probability of 0.1.

This probability can be overridden from the test properties as shown below:

```
class=example.MyTest
getProb=0.8
```

In this example, the `get` probability is 0.8, and therefore the `inc` probability is 0.2. 

If the probability is not equal to 1, the test will be terminated when the `Timestep` code is generated during the test start.

## ThreadState

A Simulator test instance is shared between all timestep-threads for that test and only on the test instance level where there
 was a state. But in some cases you want to track the state for each timestep-thread. Of course a thread-local can be used for 
 this, but the Simulator has a more practical and faster mechanism, `ThreadState`.

In the following code example, a `ThreadState` is defined that tracks the number of increments per thread:

```java
import com.hazelcast.Simulator.test.BaseThreadState
...

public class MyTest extends AbstractTest{
  public int countersLength; 

  private AtomicLong counter;

  @Setup public void setup(){
    this.counter = targetInstance.getAtomicLong("counter");
  }

  @TimeStep public void inc(ThreadState state){
    counter.incrementAndGet();
    state.increments++;
  }

  public class ThreadState extends BaseThreadState{
    long increments;
  }
}
```

In this example, tracking the number of increments is not that interesting since nothing is done with it. But it can be used to
 verify that the data structure under the test (`IAtomicLong` in this case) is working correctly. Please see the
  [Verification section](#verification) for more information.

The class of the `ThreadState` is determined by timestep code-generator and it will automatically create an instance of this class 
per timestep-thread. This instance will then be passed to each invocation of the timestep method in that timestep-thread. This
 means that you do not need to deal with more expensive thread-locals.

Extending the `BaseThreadState` class is the recommended way to define your own `ThreadState` because it provides various random 
utility methods that are needed frequently.

However, `ThreadState` does not need to extend `BaseThreadState`. `ThreadState` can be any class as long as it has a no-arg 
constructor, or it has a constructor with the type of the enclosing class as argument (a non-static inner class). `ThreadState` 
class unfortunately needs to be a public class due to the code generator. But the internals of the class do not require any 
special treatment.

Another restriction is that all `timestep`, `beforeRun` and `afterRun` methods (of the same execution group) need to have the 
same type for the `ThreadState` argument. So the following is not valid:

```java
public class MyTest extends AbstractTest{

  @TimeStep public void inc(IncThreadState state){
    counter.incrementAndGet();
    state.increments++;
  }

  @TimeStep public void get(GetThreadState list){
    counter.get();
  }
  
  public class IncThreadState{long increments;}
  public class GetThreadState{}
}
```

It is optional for any `timestep`, `beforeRun`, and `afterRun` methods to declare this `ThreadState` argument. So the following 
is valid:

```java
public class MyTest extends AbstractTest{

  @TimeStep public void inc(ThreadState state){
    counter.incrementAndGet();
    state.increments++;
  }

  @TimeStep public void get(){
    counter.get();
  }

  public class ThreadState extends BaseThreadState{
    long increments;
  }
}
```

The reason of having a single test instance shared between all threads, instead of having a test instance per thread (and 
dropping the need for the `ThreadState`) is that it will be a lot more cache friendly. It is not the test instance which 
needs to be put into the cache, everything referred from the test instance.

Another advantage is that if there is a shared state, it is easier to share it; for example, keys to select from for a `map.get`
 test between threads, instead of each test instance generating its own keys (and therefore increasing memory usage). In the
  future a `@Scope` option will probably be added so that you can choose if each thread gets its own test instance or that the
   test instance is going to be shared.

## AfterRun and BeforeRun

The timestep methods are called by a timestep-thread and each thread will do a loop over its timestep methods. In some cases 
before this loop begins or after this loop ends, some additional logic is required. For example initialization of the `ThreadState` 
object is needed when the loop starts, or updating some shared state when the loop completes. This can be done using `beforeRun`
 and `afterRun` methods. Multiple `beforeRun` and `afterRun` methods can be defined, but the order of their execution is 
 unfortunately not defined, so be careful with that.

The `beforeRun` and `afterRun` methods accept the `ThreadState` as an argument, but this argument is allowed to be omitted. 

In the following example, `beforeRun` and `afterRun` methods are defined that log when the timestep thread starts, and log when 
it completes. It also writes the number of increments the timestep thread executed:

```java
public class MyTest extends AbstractTest{
  public int countersLength; 

  private AtomicLong counter;

  @Setup public void setup(){
    this.counter = targetInstance.getAtomicLong("counter");
  }

  @BeforeRun public void beforeRun(ThreadState state){
    System.out.println(Thread.currentThread().getName()+" starting");
  }

  @TimeStep public void inc(ThreadState state){
    counter.incrementAndGet();
    state.increments++;
  }

  @AfterRun public void afterRun(ThreadState state){
    System.out.println(Thread.currentThread().getName()+
      " completed with "+state.increments+" increments");
  }

  public class ThreadState extends BaseThreadState{
    long increments;
  }
}
```

## Verification

Once a Simulator test is completed, you can do the verifications using the `@Verify` annotation. In the case of `IAtomicLong.inc` 
test, you could count the number of increments per thread. After the test completes, you can verify the total count of expected 
increments and the actual number of increments.

```java
public class MyTest extends AbstractTest{
  private IAtomicLong counter;
  private IAtomicLong expected;

  @Setup public void setup(){
    this.counter = targetInstance.get("counter");
    this.expected = targetInstance.get("expected");  
  }

  @TimeStep public void inc(ThreadState state){
      state.increments++;
      counter.incrementAndGet();
  }
 
  @AfterRun public void afterRun(ThreadState state){
     expected.addAndGet(state.increments);
  }
  
  @Verify public void verify(){
    assertEquals(expected.get(), counter.get())
  }
  
  public class ThreadState extends BaseThreadState {
    long increments;
  }
}
```

In the above example once the timestep-loop completes, each timestep-thread will call the `afterRun` method and add the actual 
number of increments to the `expected` IAtomicLong object. In the `verify` method the expected number of increments is compared
 with the actual number of increments.

The example also shows we make use of the JUnit's `assertEquals` method. So you can use JUnit or any other framework that can 
verify behaviors. It is even fine to throw an exception.

It is allowed to define zero, one or more verify methods.

By default the verify will run on all workers, but it can be configured to run on a single worker using the global property on 
the `@Verify` annotation.

## TearDown

To automatically remove created resources, a `tearDown` can be added. It depends on the situation if this is needed at all for
 your test because in most cases the workers will be terminated anyway after the Simulator test completes. But just in case you
  need to tear down the resources, it is possible.

In the following example the `tearDown` is demonstrated:

```
public class MyTest extends AbstractTest{
  private IAtomicLong counter;

  @Setup public void setup(){
    counter = targetInstance.getAtomicLong("c");
  }

  @TimeStep public void inc(){
    counter.inc();
  }

  @TearDown public void tearDown(){
    counter.destroy();
  }
}
```

By default the `tearDown` is executed on all participating workers, but can be influenced using the global property as shown below:

```
public class MyTest extends AbstractTest{
  private IAtomicLong counter;

  @Setup public void setup(){
    counter = targetInstance.getAtomicLong("c");
  }

  @TimeStep public void inc(){
    counter.inc();
  }

  @TearDown(global=true) public void tearDown(){
    counter.destroy();
  }
}
```

When `global` is set to `true`, only one worker is going to trigger the `destroy`. It is allowed to define multiple `tearDown` methods.

## Latency Testing

Out of the box the timestep code generator emits code for tracking latencies using the excellent `HdrHistogram` library. An `HDR` 
file is created for each timestep method. This way you can, for example, compare a `Map.put` with a `Map.get` latency from the same test. 

By default the timestep-threads will loop over the timestep methods as fast as they can and this is great for throughput testing.
 As a bonus you get an impression of the latency for that throughput. However, for a proper latency test, you want to control the 
 rate and measure the latency for that rate. Luckily using the Simulator this is very easy. 

```
class=example.MyTest
threadCount=10
interval=10ms
```

If for discussion sake we assume the `MyTest` has a single timestep method called `inc`, then with the above configuration each 
timestep thread will make one `inc` call every 100ms. Because there are 10 threads, we get an interval per request of 10ms. 
The interval is configured per load generating client/member. So if there are two workers generating load, then globally a 
call is made every 5ms. So the only thing you need to take care of it to specify the interval correctly on the worker level,
and Simulator will calculate the correct delay per worker thread.

Another way to configure the throughput is using the `ratePerSecond` property. Please see the following example:

```
class=example.MyTest
threadCount=10
ratePerSecond=100
```

In this case each thread will make 10 requests per second. The `ratePerSecond` under the hood is transformed to interval, so it 
is a matter of convenience which one is preferred.

### Coordinated Omission

By default the Simulator prevents the coordinated omission problems by using the expected start time of a request instead of the 
actual time. So instead of trying to do some kind of a repair after it happened, the Simulator actually prevents the problem
 happening in the first place. Similar technique is used in [JLBH](http://www.rationaljava.com/2016/04/jlbh-introducing-java-latency.html).

If you are interested in the impact of coordinated omission, the protection against it can be disabled using the `accountForCoordinatedOmission` property:
```
class=example.MyTest
threadCount=10
ratePerSecond=100
accountForCoordinatedOmission=false
```

Be extremely careful when setting this property to false and publishing the results. Because the number will be a lot more positive
 than they actually are.

The rate of doing requests is controlled using the `Metronome` abstraction and a few flavors are available. One very interesting 
metronome is the `ConstantCombinedRateMetronome`. By default each timestep-thread will wait for a given amount of time for the
 next request and if there is some kind of an obstruction, e.g., a `map.get` is obstructed by a fat entry processor, a bubble 
 of requests is built up that is processed as soon as the entry processor has completed.

Instead of building up this bubble, the `ConstantCombinedRateMetronome` can be used. If one thread is obstructing while it wants 
to do a `get`, other timestep-threads from the same execution group will continue with the requests this timestep thread was 
supposed to do. This way the bubble is prevented; unless all timestep threads from the same execution group are obstructed.

The `ConstantCombinedRateMetronome` can be configured as shown below:

```
class=example.MyTest
threadCount=10
ratePerSecond=100
metronomeClass=com.hazelcast.simulator.worker.metronome.ConstantCombinedRateMetronome
```

### Jitter
To measure jitter caused by the OS/JVM it is possible to active a Jitter thread using:
```
class=example.MyTest
threadCount=10
ratePerSecond=100
recordJitter=true
```
This thread will do nothing else than measuring time and recording it in a probe. The content of this probe results in hdr files
and can be visualized using the benchmark report generator.

By default jitter greater or equal 1000ns is recorded, but can b configured using the `recordJitterThresholdNs` property:
```
class=example.MyTest
threadCount=10
ratePerSecond=100
recordJitter=true
recordJitterThresholdNs=2_000
```
To disable the threshold, set `recordJitterThresholdNs` to 0. Warning: if the `recordJitterThresholdNs` is set to a value higher
than zero, the latency distribution looks distorted because only the outliers are recorded and not the samples below the threshold.

Measuring jitter is only recommended when doing a latency test because you will loose 1 core. Each test instance will create its 
own jitter thread (if the test is configured to use a jitter thread). So it extremely unlike that you want to run tests in 
parallel with this feature enabled.

## Logging

In some cases, especially when debugging, logging is required. One easy way to add logging is to add the logging into the timestep 
method. But this can be inefficient and it is frequently noisy. Using some magic properties logging can be enabled on any timestep
 based Simulator test. There are two types of logging:

- **frequency based**; for example every 1000th iteration, each timestep thread will log where it is.
- **time rate based**; for example every 100ms each timestep thread will log where it is. Time rate based is quite practical 
because you do not get swamped or a shortage of log entries, like the frequency based one.

You can configure frequency based logging as shown below:
```
class=example.MyTest
logFrequency=10000
```

In this example, every 10000 iteration, a log entry is made per timestep thread.

You can configure time rate based logging as shown below:

```
class=example.MyTest
logRateMs=100
```

In this example, at most every 100ms, a log entry is made per timestep thread.

## Code Generation

The timestep methods rely on code generation, that is why a JDK is required to run a timestep based test. The code is generated 
on the fly based on the test and its test parameters. The philosophy is that you should not pay the price for something that is
 not used. For example, if there is a single timestep method, no randomization/switch-case is needed to execute the right method.
  If no logging is configured, no logs are generated. 

This way many features can be added to the timestep test without impacting the performance if the actual feature is not used.

The generator timestep worker code can be found in the worker directory. Feel free to have a look at it and send any suggestions 
how it can be improved.

Currently there is no support yet for dead code elimination.

## Stopping a Test

By default a Simulator test will run for a given amount of time using the duration property. Please see the following example:

```
coordinator --duration 5m test.properties
```

In this example, the test will run for five minutes. In some cases you need more control on when to stop. Currently there are 
following options available:

- **Configuring the number of iterations**:
  The number of iterations can be specified using the test properties:

   ```
   class=example.MyTest
   iterations=1000000
   ```

   In this case the test will run for 1000k iterations. 
 
- **`StopException` to stop a single thread**: When a timestep thread wants to stop, it can throw a `StopException`. This 
exception does not lead to a failure of the test. It also has no influence on any other timestep thread.

- **`TestContext.stop` to stop all timestep threads**: All timestep threads for a given period on a single worker can be 
stopped using the `TestContext.stop` method. 


In all cases, Coordinator will wait for all timestep threads of all workers to complete. If a duration has been specified, the
test will not run longer than this duration.

## Total Lifecycle of Calls on the Test

- setup
- prepare local
- prepare global
  - timestep-thread:before run
  - timestep-thread:timestep ...
  - timestep-thread:after run
- local verify
- global verify
- local teardown
- global teardown


# Report Generator

Once a benchmark has been executed, an HTML report can be generated using the `benchmark-generator` tool. This tool requires 
Gnuplot 4+ and Pyton 2.7 to be installed for generating the diagrams.

Assume that a benchmark has been executed and the directory `2016-08-02__22_08_09` has been created. To create a report for that 
benchmark, you can use the following command:

```
benchmark-generator mybenchmark 2016-08-02__22_08_09
```

The name `mybenchmark` is output directory's name. The generated report contains detailed throughput and latency information. 
If `dstats` information is available, it shows detailed information about resource utilization such as network, CPU, and memory.

The `benchmark-generator` tool is also able to make comparisons between two or more benchmarks. You can list the benchmark 
directories to be compared as shown below:

```
benchmark-generator mybenchmark 2016-08-02__22_08_09 2016-08-02__22_18_21
```



# Simulator Properties File Description

The file `simulator.properties` is placed at the `conf` folder of your Hazelcast Simulator. This file is used to prepare the 
Simulator tests for their proper executions according to your business needs.

![](images/NoteSmall.jpg)***NOTE:*** *Currently, the main focuses are on the Simulator tests of Hazelcast on Amazon EC2 and
 Google Compute Engine (GCE). For the preparation of `simulator.properties` for GCE, please refer to the
  [Setting Up For GCE section](#setting-up-for-google-compute-engine). The following `simulator.properties` file description is mainly for Amazon EC2.*


# Mail Group

Please join the mail group if you are interested in using or developing Hazelcast.

[http://groups.google.com/group/hazelcast](http://groups.google.com/group/hazelcast)

# License

Hazelcast Simulator is available under the Apache 2 License.

# Copyright

Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.

Visit [www.hazelcast.com](http://www.hazelcast.com/) for more info. Also, see the Hazelcast Simulator chapter in the Reference 
Manual at [http://hazelcast.org/documentation/](http://hazelcast.org/documentation/). Docs for latest version can be found [here](http://docs.hazelcast.org/docs/latest/manual/html-single/index.html#hazelcast-simulator)
