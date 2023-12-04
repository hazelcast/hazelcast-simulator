# Hazelcast Simulator

Hazelcast Simulator is a production simulator used to test Hazelcast and Hazelcast-based applications in clustered
environments.
It also allows you to create your own tests and perform them on your Hazelcast clusters and applications that are
deployed to
cloud computing environments. In your tests, you can provide any property that can be specified on these environments (
Amazon EC2 or your own environment): properties such as hardware specifications, operating system, Java version, etc.

Hazelcast Simulator allows you to add potential production problems, such as real-life failures, network problems,
overloaded CPU,
and failing nodes to your tests. It also provides a benchmarking and performance testing platform by supporting
performance
tracking and also supporting various out-of-the-box profilers.

You can use Hazelcast Simulator for the following use cases:

- In your pre-production phase to simulate the expected throughput/latency of Hazelcast with your specific requirements.
- To test if Hazelcast behaves as expected when you implement a new functionality in your project.
- As part of your test suite in your deployment process.
- When you upgrade your Hazelcast version.

Hazelcast Simulator is available as a downloadable package on the
Hazelcast <a href="https://hazelcast.com/open-source-projects/downloads/" target="_blank">website</a>.
Please refer to the [Quickstart](#quickstart) to start your Simulator journey.

- [Quickstart](#quickstart)
    * [Install](#install)
    * [Creating a benchmark](#creating-a-benchmark)
    * [Provisioning the environment](#provisioning-the-environment)
    * [SSH to nodes](#ssh-to-nodes)
    * [Running a test.](#running-a-test)
    * [What's next](#whats-next)
- [Key Concepts and Terminology](#key-concepts-and-terminology)
- [Define test scenario](#define-test-scenario)
    * [TestSuite configuration](#testsuite-configuration)
        + [Specify the test environment and non-class-specific parameters](#specify-the-test-environment-and-non-class-specific-parameters)
        + [Specify test class(es) and number of threads per worker](#specify-test-classes-and-number-of-threads-per-worker)
        + [Setting up operations frequency](#setting-up-operations-frequency)
        + [Configuring parameters](#configuring-parameters)
        + [Latency Testing](#latency-testing)
    * [Controlling the Cluster Layout](#controlling-the-cluster-layout)
        + [Set number of members and clients](#set-number-of-members-and-clients)
        + [Control distribution of workers over machines](#control-distribution-of-workers-over-machines)
            - [Default distribution algorithm](#default-distribution-algorithm)
            - [Reserving machines for members only](#reserving-machines-for-members-only)
                * [Order of the IP addresses](#order-of-the-ip-addresses)
        + [Running tests against an already running cluster](#running-tests-against-an-already-running-cluster)
        + [Running tests against a cluster in Hazelcast Cloud](#running-tests-against-a-cluster-in-hazelcast-cloud)
    * [Controlling the Hazelcast Configuration](#controlling-the-hazelcast-configuration)
        + [IP addresses and other configuration auto-filling](#ip-addresses-and-other-configuration-auto-filling)
- [Run the test](#run-the-test)
    * [Configure test duration](#configure-test-duration)
    * [Specify TestSuite file to be used](#specify-testsuite-file-to-be-used)
    * [Installing Simulator on remote machines](#installing-simulator-on-remote-machines)
- [Report generation](#report-generation)
    * [Basics](#basics)
    * [Generate comparison reports](#generate-comparison-reports)
    * [Extensive reports](#extensive-reports)
    * [Warmup and cooldown](#warmup-and-cooldown)
- [Simulator Properties reference](#simulator-properties-reference)
- [Advanced topics](#advanced-topics)
    * [Writing a Simulator test](#writing-a-simulator-test)
        + [Adding properties](#adding-properties)
        + [ThreadState](#threadstate)
        + [AfterRun and BeforeRun](#afterrun-and-beforerun)
        + [Verification](#verification)
        + [TearDown](#teardown)
        + [Complete Lifecycle of Calls on the Test](#complete-lifecycle-of-calls-on-the-test)
        + [Stopping a Test](#stopping-a-test)
        + [Code Generation](#code-generation)
    * [Profiling your Simulator Test](#profiling-your-simulator-test)
    * [GC analysis](#gc-analysis)
    * [Reducing Fluctuations](#reducing-fluctuations)
    * [Enabling Diagnostics](#enabling-diagnostics)
    * [Logging](#logging)
    * [Running multiple tests in parallel](#running-multiple-tests-in-parallel)
    * [Various forms of testing](#various-forms-of-testing)
- [Get Help](#get-help)

# Quickstart

This is a 5 minute tutorial where that shows you how to get Simulator running on your local machine.
Also contains pointers where to go next.

## Install

1. Checkout the Simulator git repository:

    ```shell
    git clone https://github.com/hazelcast/hazelcast-simulator.git
    ```


2. Install tools
    - JDK 8+
    - [Maven](https://maven.apache.org/download.cgi)
    - [Terraform](https://developer.hashicorp.com/terraform/downloads)
    - [Python 3.8 or newer](https://www.python.org/downloads/)
    - Python3-pip
    - python3-gobject
    - rsync

3. Install Python libraries:

    ```shell
    pip3 install -U ansible pyyaml matplotlib signal-processing-algorithms
    ```

   `signal-processing-algorithms` is only needed when you are going to do performance regression testing. The library is
   used for change point detection.


4. Build Simulator:

   ```
   cd hazelcast-simulator
   ./build
   ```
   This will automatically build the Java code, download the artifacts and prepare the simulator for usage.


5. Add the Simulator to your path

   Open `~/.bash_profile` and add the following line:
   ```
   PATH=<path-to-simulator>/bin/:$PATH
   ```

## Creating a benchmark

The first step is to create a benchmark, which can be done using the `perftest` tool.

   ```shell
   perftest create myproject
   ```

This will create a fully configured benchmark that will run in EC2.

There are various benchmark templates. These can be accessed using:

   ```shell
   perftest create --list 
   ```

And a benchmark using a specific benchmark can be created using:

   ```shell
   perftest create --template <templatename> myproject
   ```

In the future more templates will be added.

## Provisioning the environment

Simulator makes use of Terraform for provisioning. After you have created a benchmark using the
`perftest create` command, you want to edit the `inventory_plan.yaml`. This is where you can configure the
type of instances, the number etc. The specified `cidr_block` will need to be updated to prevent conflicts.

To provision the environment, you will first need to configure your AWS credentials:

In your `~/.aws/credentials` file you need something like this:

```
[default]
aws_access_key_id=AKIAIOSFODNN7EXAMPLE
aws_secret_access_key=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
```

Or when you make use of a token:

```
[default]
aws_access_key_id=AKIAIOSFODNN7EXAMPLE
aws_secret_access_key=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
aws_session_token=AQoEXAMPLEH4aoAH0gNCAPyJxz4BlCFFxWNE1OPTgk5TthT+FvwqnKwRcOIfrRh3c/LTo6UDdyJwOOvEVPvLXCrrrUtdnniCEXAMPLE/IvU1dYUg2RVAJBanLiHb4IgRmpRV3zrkuWJOgQs8IZZaIv2BXIa2R4OlgkBN9bkUDNCJiBeb/AXlzBBko7b15fjrBs2+cTQtpZ3CYWFXG8C5zqx37wnOE49mRl/+OtkIKGO7fAE
```

Or alternatively you can use `aws sso login` and autorize your terminal via SSO.

To apply the configuration on an existing environment, execute the following command from within the benchmark
directory:

   ```shell
   inventory apply
   ```

After the apply command has completed, a new file `inventory.yaml` file is created containing
created machines. This is an Ansible specific file. Simulator uses Ansible to configure the
remote machines.

To install Java on the remote machines call:

   ```shell
   inventory install java
   ```

You can pass a custom URL to configure the correct JVM. To get a listing of examples URL's call:

   ```shell
   inventory install java --examples 
   ```

And run the following to install a specific Java version.

   ```shell
   inventory install java --url https://corretto.aws/downloads/latest/amazon-corretto-17-x64-linux-jdk.tar.gz 
   ```

This command will update the `JAVA_HOME`/`PATH` on the remote machine to reflect the last installed Java version.

Install the Simulator:

   ```shell
   inventory install simulator
   ```

To destroy the environment, call the following:

   ```shell
   inventory destroy
   ```

## SSH to nodes

To SSH into your remote nodes, the following command can be used from the test directory:

```
ssh -i key <username>@<ip>
```

## Running a test.

In the generated benchmark directory, a `tests.yaml` file is created and it will contain something like this:

```yaml
     - name: write_only
       duration: 300s
       repetitions: 1
       clients: 1
       members: 1
       driver: hazelcast5
       version: maven=5.0
       client_args: -Xms3g -Xmx3g
       member_args: -Xms3g -Xmx3g
       loadgenerator_hosts: loadgenerators
       node_hosts: nodes
       verify_enabled: False
       performance_monitor_interval_seconds: 1
       warmup_seconds: 0
       cooldown_seconds: 0
       test:
         class: com.hazelcast.simulator.tests.map.IntByteMapTest
         threadCount: 40
         getProb: 0
         putProb: 1
         keyCount: 1_000_000
```

To run the benchmark

   ```shell
   perftest run
   ```

## What's next

The quickstart was to just get you up and running. In order to do some real performance testing, you'll probably need
to:

* [Define test scenario](#define-test-scenario) - specify how many puts/gets to use, how many entries to preload, how
  big the values should be, latency vs. throughput test etc.
* [Configure cluster](#controlling-the-cluster-layout) - Hazelcast version, configuration of Hazelcast itself, number of
  members and clients, number of threads per client, GC options etc.
* [Run the test](#run-the-test) - set test duration, select which test scenario to be run etc.
* [Setup the testing environment](#set-up-cluster-environment) - run it on on-premise machines, in AWS, configuring for
  running clusters in OpenShift, Kubernetes etc.
* [Create better charts](#report-generation) - create charts with multiple runs being compared, adjust warmup and
  cooldown periods, adjust legents etc.

You can use the following channels for getting help from Hazelcast:

* [Hazelcast mailing list](http://groups.google.com/group/hazelcast)
* [Slack](https://slack.hazelcast.com/) for chatting with the
  development team and other Hazelcast users.
* [Stack Overflow](https://stackoverflow.com/tags/hazelcast)

# Key Concepts and Terminology

The following are the key concepts mentioned with Hazelcast Simulator.

- **Test** - A test class for the functionality you want to test, e.g. a Hazelcast map. This test class looks similar to
  a JUnit
  test, but it uses custom annotations to define methods for different test phases (
  e.g. `@Setup`, `@Warmup`, `@Run`, `@Verify`).

- **TestSuite** - A yaml file that contains the name of the `Test` classes and the properties you want to set on
  those `Test`
  class instance. A `TestSuite` contains one or multiple tests. It can also contain the same `Test` class with different
  names and configurations.

- **Worker** - This term `Worker` is used twice in Simulator.

    - **Simulator Worker** - A Java Virtual Machine (JVM) responsible for running the configured `Tests`. It can be
      configured to
      spawn a Hazelcast client or member instance, which is used in the tests. We refer to this `Worker` in the context
      of a Simulator
      component like `Agent` and `Coordinator`.

    - **Test Worker** - A Runnable implementation to increase the test workload by spawning several threads in
      each `Test` instance.
      We refer to this `Worker` in the context of a `Test`, e.g. how many worker threads a `Test` should create.

- **Agent** - A JVM responsible for managing client and member `Workers`. There is always one `Agent` per physical
  machine, no
  matter how many `Workers` are spawned on that machine. It serves as communication relay for the `Coordinator` and
  monitoring
  instance for the `Workers`.

- **Coordinator** - A JVM that can run anywhere, such as on your local machine. The `Coordinator` is actually
  responsible for
  running the `TestSuite` using the `Agents` and `Workers`. You configure it with a list of `Agent` IP addresses, and
  you run it by
  executing a command like "run this testsuite with 10 member worker and 100 client worker JVMs for 2 hours".

- **Coordinator Machine** - a machine on which you execute the `Coordinator` (see above). This is the place typically
  where the user interacts
  with Simulator commands. Typically your local computer but can be installed anywhere.

- **Coordinator Remote** - A JVM that can run anywhere, such as on your local machine. The `CoordinatorRemote` is
  responsible for
  sending instructions to the Coordinator. For basic simulator usages the remote is not needed, but for complex
  scenarios such
  as **rolling upgrade** or **high availability** testing, a much more interactive approach is required. The coordinator
  remote
  talks to the coordinator using TCP/IP.

- **Provisioner** - Spawns and terminates cloud instances, and installs Hazelcast Simulator on the remote machines. It
  can be used
  in combination with EC2 (or any other cloud provider), but it can also be used in a static setup, such as a local
  machine or a cluster of
  machines in your data center.

- **Failure** - An indication that something has gone wrong. Failures are picked up by the `Agent` and sent back to
  the `Coordinator`.

- **simulator.properties** - The configuration file you use to adapt the Hazelcast Simulator to your business needs (
  e.g. cloud
  provider, SSH username, Hazelcast version, Java profiler settings, etc.).

# Define test scenario

This section describes how you can control *what* the test should do - should it do only PUTs or also GETs and if so,
in which ratio? Or should it execute SQL queries etc.?

## TestSuite configuration

The TestSuite defines the Simulator Tests which are executed during the Simulator run.
The TestSuite configuration is a simple YAML file which contains `key-value` pairs. The common
name of the file is `tests.yaml` which is also the default (e.g. generated by `perftest create` as seen
in [Quickstart](#quickstart)).

> We will use `tests.yaml` file name through the rest of the documentation for the TestSuite configuration. However,
> the file can be named arbitrarily. See the [Specify TestSuite file to be used](#specify-testsuite-file-to-be-used)
> section on details how to specify
> different properties file.

When you open up the default (generated by `perftest create`) `tests.yaml` file, you'll see (as well as a `write_only`
variant):

```yaml
- name: read_only
  repetitions: 1
  duration: 300s
  clients: 1
  members: 1
  loadgenerator_hosts: loadgenerators
  node_hosts: nodes
  driver: hazelcast5
  version: maven=5.1
  client_args: >
    -Xms3g
    -Xmx3g
  member_args: >
    -Xms3g
    -Xmx3g
  performance_monitor_interval_seconds: 1
  verify_enabled: True
  warmup_seconds: 0
  cooldown_seconds: 0
  license_key: <add_key_here_if_using_ee>
  parallel: False
  test:
    - class: com.hazelcast.simulator.tests.map.IntByteMapTest
      name: MyByteTest
      threadCount: 40
      getProb: 1
      putProb: 0
      keyCount: 1_000_000
```

Let's explain the lines one by one.

### Specify the test environment and non-class-specific parameters

Each section within the `tests.yaml` file contains properties for the environment the defined tests for that section
should be conducted in.

| Property                               | Example value    | Description                                                                                                                                 |
|----------------------------------------|------------------|---------------------------------------------------------------------------------------------------------------------------------------------|
| `name`                                 | `read_only`      | The name of the test suite (overriden by test-specific values                                                                               |
| `repititions`                          | `1`              | The number of times this test suite should run (1 or more)                                                                                  |
| `duration`                             | `300s`           | The amount of time this test suite should run for (45m, 1h, 2d, etc.)                                                                       |
| `clients`                              | `1`              | The number of Hazelcast Clients to use in this test suite (hosted on `loadgenerator_hosts`                                                  |
| `members`                              | `1`              | The number of Hazelcast Members to use in this test suite (hosted on `node_hosts`)                                                          |
| `loadgenerator_hosts`                  | `loadgenerators` | Defines the host for Clients, based on either `loadgenerators` or `nodes`, allowing both separate and mixed client/member setups            |
| `node_hosts`                           | `nodes`          | Defines the host for Members - this should generally always be `nodes`, and only `loadgenerator_hosts` should be changed for mixed testing. |
| `driver`                               | `hazelcast5`     | The Hazelcast Driver to use - for 5.0+ testing, this is either `hazelcast5` or `hazelcast-enterprise5` for OS or EE respectively            |
| `version`                              | `maven=5.1`      | The Hazelcast version to use - typically provided by maven, i.e. `maven=5.3.0-SNAPSHOT`                                                     |
| `client_args`                          | `-Xms3g -Xmx3g`  | The command-line Java parameters passed to all clients in this test suite                                                                   |
| `member_args`                          | `-Xms3g -Xmx3g`  | The command-line Java parameters passed to all members in this test suite                                                                   |
| `performance_monitor_interval_seconds` | `1`              | The interval of the Simulator performance monitor                                                                                           |
| `verify_enabled`                       | `True`           | Defines whether tests should be verified after completion or not (default true)                                                             |
| `warmup_seconds`                       | `0`              | The number of seconds from the start of the test to exclude in reporting (only used for report generation)                                  |
| `cooldown_seconds`                     | `0`              | The number of seconds before the end of the test to exclude in reporting (only used for report generation)                                  |
| `license_key`                          | `your_ee_key`    | The Hazelcast Enterprise Edition license to use in your test, if using `hazelcast-enterprise5` drivers                                      |
| `parallel`                             | `True`           | Defines whether tests should be run in parallel when multiple tests are defined within 1 suite (default false)                              |

### Specify test class(es) and number of threads per worker

Beyond the environment parameters above, under the `test` section we define the actual tests to run. The first three
properties shown in the above example are built-in "magic" properties of Simulator.

| Property      | Example value                                      | Description                                                                                                                                                                                                                         |
|---------------|----------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `class`       | `com.hazelcast.simulator.tests.map.IntByteMapTest` | Defines the fully qualified class name for the Simulator Test. Used to create the test class instance on the Simulator Worker. This is the only mandatory property which has to be defined.                                         |
| `name`        | `MyByteTest`                                       | Defines a unique name for this Test. This property is only required when running multiple tests on the same test class, without it only 1 test will run per class type (as the class name is used as the name if not defined here). |
| `threadCount` | `40`                                               | Defines how many threads are running the Test methods in parallel. In other words, defines the number of worker threads for Simulator Tests which use the `@RunWithWorker` annotation.                                              |

> :books: For details about available values for `class`, refer to the provided classes in the [drivers](drivers)
> directory or the [Writing a Simulator test](#writing-a-simulator-test) section.

### Setting up operations frequency

Next up, there's some properties with special functionality, which all have their
names ending with `Prob` (short for "probability"), such as `getProb` and `putProb`.

These properties conform to the format `<methodName>Prob: <probability>`, where:

* `<methodName>` corresponds to the name of a timestep method (a method annotated with `@TimeStep` annotation) in the
  test class
  configured with `class` property. For example, the `com.hazelcast.simulator.tests.map.IntByteMapTest` test contains
  the following methods:

  ```java
  @TimeStep
  public void put(ThreadState state) {
    map.put(state.randomKey(), state.randomValue());
  }
  
  @TimeStep
  public void get(ThreadState state) {
    map.get(state.randomKey());
  }
  ```

* `<probability>` is a float number from `0` to `1` that sets a probability of execution for the method.
  For example, a probability of `0.1` means a 10 % probability for execution.

As a complete example, a `putProb: 0.1` property sets the probability of execution of the `put` method to 10 %.
In other words, out of all the things being done by the test, 10 % will be PUTs. This is the basic method for
controlling the ratio of operations.
For example, if you want to execute 80 % GETs and 20 % PUTs with `IntByteMapTest` you would set `getProb: 0.8`
and `putProb: 0.2`.

A special case of probability value is `-1` which means "calculate the remaining probability to 1". An example:

```yaml
putProb: 0.1
setProb: 0.2
getProb: -1
```

The above properties result in 10 % PUT operations, 20 % SET operations, and (`1-0.1-0.2=0.7`) 70 % GET operations.

### Configuring parameters

All the other properties are values passed directly to the test class and are usually used for adjusting
parameters of the test such as number of entries being preloaded in the Map, size of the value etc. Each test class
has its own set of options, so you have to look at the source code of the test class for the available parameters
and their meaning.

The property must match a public field in the test class. If a defined property cannot be found in the Simulator Test
class or the value cannot be converted to the according field type, a BindException is thrown. If there is no property
defined
for a public field, its default value will be used.

Let's continue using `com.hazelcast.simulator.tests.map.IntByteMapTest` as an example.
It contains the following public fields:

```java
public class IntByteMapTest extends HazelcastTest {
    public int keyCount = 1000;
    public int minSize = 16;
    public int maxSize = 2000;
    ... and more
}
``` 

Hopefully the names of the properties are self-explanatory. Therefore, if we wanted to change the test scenario
and preload 1 million entries with a value size of exactly 10 KB, we would edit the `tests.yaml` file as follows:

```yaml
  test:
    - class: com.hazelcast.simulator.tests.map.IntByteMapTest
      # probabilites and thread count settings
      minSize: 10_000
      maxSize: 10_000
      keyCount: 1_000_000
```

### Latency Testing

> In general, when doing performance testing, you should always distinguish between throughput and latency testing.
> * **Throughput test** - stress out the system as much as possible and get as many operations per second as possible.
> * **Latency test** - measure operation latencies while doing _a fixed number of operations per second_.

By default the timestep-threads operate in throughput testing mode - they will loop over the timestep methods as fast as
they can.
As a bonus you get an impression of the latency for that throughput.
However, for a proper latency test, you want to control the rate and measure the latency for that rate. Luckily this is
very easy with the Simulator.

You can configure the fixed number of operations per second using following properties in the `test` section
of `tests.yaml`:

* `ratePerSecond: <X>` - where `<X>` is a desired number of operations per second per **load generating client/member
  ** (not worker thread!).
  Example: if in your test, you configure 5 clients and you want to stress the cluster with 500 000 operations per
  second,
  you set `ratePerSecond: 100000`, because 5 clients times 100 000 ops = desired 500 K ops.

* `interval: <Y>` - where `<Y>` is the time interval between subsequent calls per **load generating client/member** (not
  worker thread!).
  Example: if in your test, you configure 5 clients and you want to stress the cluster with 500 000 operations per
  second,
  you set `interval: 100us`, because 5 clients times 100 000 ops = desired 500 K ops.

> Accepted time units for the `interval` property:
> * `ns` - nanoseconds
> * `us` - microseconds
> * `ms` - milliseconds
> * `s` - seconds
> * `m` - minutes
> * `h` - hours
> * `d` - days

> :books: From the descriptions above, you can see that if you set the number of operations per second, different values
> of the `threadCount`
> property don't affect it. The formulas are:
> * `number of clients` * `ratePerSecond` = `total number of operations per second`
> * `number of clients` * `(1000 / interval_in_ms)` = `total number of operations`
>
> Both ways work exactly the same and it's just a matter of preference which one you use.

## Controlling the Cluster Layout

Hazelcast has two basic instance types: member and client. The member instances form the cluster and client instances
connect to
an existing cluster. Hazelcast Simulator can spawn Workers for both instance types. You can configure the number of
member and
client Workers and also their distribution on the available remote machines.

> All configuration about the cluster layout is managed through the `inventory_plan.yaml` file which handles the actual
> provisisiong, as well as the `tests.yaml` file which handles the allocation of workers between hosts.

### Set number of members and clients

Use the options `--members` and `--clients` to control how many member and client Workers you want to have. The
following command
creates a cluster with four member Workers and eight client Workers (which connect to that cluster).

```
coordinator --members 4 --clients 8
```

A setup without client Workers is fine, but out of the box it won't work without member Workers.

### Control distribution of workers over machines

Through this section, we'll assume that we have 3 remote machines that we're going to use. In other words,
there are 3 IP addresses specified in the `inventory.yaml` like this:

```yaml
nodes:
  hosts:
    21.333.44.55:
      ansible_ssh_private_key_file: key
      ansible_user: ec2-user
      private_ip: 10.0.0.1
    22.333.44.55:
      ansible_ssh_private_key_file: key
      ansible_user: ec2-user
      private_ip: 10.0.0.2
    23.333.44.55:
      ansible_ssh_private_key_file: key
      ansible_user: ec2-user
      private_ip: 10.0.0.3
```

> :books: The `inventory.yaml` file is generated after AWS machines have been provisioned using `inventory apply`.

#### Default distribution algorithm

The Workers will be distributed among the available remote machines with a round robin selection. First the members are
distributed in the round robin
fassion (going through the IP addresses from the top to the bottom). Once there are no more members to be distributed,
Simulator
continues (= **not** starting from the first IP address but continuing with the next one) with distribution of the
clients. By default, the machines will
be mixed with member and client Workers. Let's see a couple of examples.

| Tests.yaml properties    | Cluster layout                                                                                                               |
|--------------------------|------------------------------------------------------------------------------------------------------------------------------|
| `members: 1, clients: 1` | <pre>10.0.0.1 - members:  1, clients:  0<br>10.0.0.2 - members:  0, clients:  1<br>10.0.0.3 - members:  0, clients:  0</pre> |
| `members: 1, clients: 2` | <pre>10.0.0.1 - members:  1, clients:  0<br>10.0.0.2 - members:  0, clients:  1<br>10.0.0.3 - members:  0, clients:  1</pre> |
| `members: 1, clients: 3` | <pre>10.0.0.1 - members:  1, clients:  1<br>10.0.0.2 - members:  0, clients:  1<br>10.0.0.3 - members:  0, clients:  1</pre> |
| `members: 2, clients: 2` | <pre>10.0.0.1 - members:  1, clients:  1<br>10.0.0.2 - members:  1, clients:  0<br>10.0.0.3 - members:  0, clients:  1</pre> |
| `members: 4, clients: 2` | <pre>10.0.0.1 - members:  2, clients:  0<br>10.0.0.2 - members:  1, clients:  1<br>10.0.0.3 - members:  1, clients:  1</pre> |

#### Reserving machines for members only

You can reserve machines for members only (which is a Hazelcast recommended setup) using the `--dedicatedMemberMachines`
flag:

```
perftest exec --dedicatedMemberMachines 2
```

The algorithm that takes the first 2 IP addresses and distributes the members only across them in a round robin fashion.
Then takes the rest of the IP addresses and distributes the clients across them, again in the round robin fashion.
Continuing our [example](#control-distribution-of-workers-over-machines):

| Tests.yaml properties + perftest flag                 | Cluster layout                                                                                                               |
|-------------------------------------------------------|------------------------------------------------------------------------------------------------------------------------------|
| `members: 2, clients: 4, --dedicatedMemberMachines 1` | <pre>10.0.0.1 - members:  2, clients:  0<br>10.0.0.2 - members:  0, clients:  2<br>10.0.0.3 - members:  0, clients:  2</pre> |
| `members: 3, clients: 4, --dedicatedMemberMachines 2` | <pre>10.0.0.1 - members:  2, clients:  0<br>10.0.0.2 - members:  1, clients:  0<br>10.0.0.3 - members:  0, clients:  4</pre> |

You cannot specify more dedicated member machines than you have available. If you define client Workers, there must be
at least
a single remote machine left (e.g. with three remote machines you can specify a maximum of two dedicated member
machines).

##### Order of the IP addresses

The order of the IP addresses matters. Simulator goes from the top to the bottom and applies the algorithm described
above
deterministically and always the same way.

That allows you to fine tune the configuration of the environment. Imagine a typical usecase where you want to run
the members on more powerful machines (e.g. more CPUs, more memory) and use lighter and cheaper (e.g. in the cloud)
machines
for the clients.

> :warning: Running multiple members on a single machines is a Hazelcast performance anti-pattern and should be avoided.
> We used it only for a demonstration of the cluster layout distribution.
> Consult [Hazelcast documentation](https://docs.hazelcast.com) for more information about the recommended setup.

### Running tests against an already running cluster

There are cases where you already have a running cluster and you want to execute performance test against it.
In other words, you don't want the Simulator to manage your members but only orchestrate the clients.
In order to do this, you need to use the internal `coordinator` command, and you have to:

* Specify `--members` to `0` - Simulator will not care about members at all, won't control their lifecycle etc.
* Put member IP addresses in the `client-hazelcast.xml` - since Simulator doesn't control the member lifecycle, it
  can't possibly know the IP addresses of the members. Therefore, you have to manually provide it through editing the
  client
  configuration. For more information about this, refer
  to [Controlling the Hazelcast configuration](#controlling-the-hazelcast-configuration).
* Specify the correct `<cluster-name>` in the `client-hazelcast.xml` - for the same reason as with IP addresses,
  you have to adjust the `<cluster-name>` configuration to match the one in the running cluster.

### Running tests against a cluster in Hazelcast Cloud

If you want to test the performance of the Hazelcast Cloud managed cluster, you follow the same setup as
described in [Running tests against an already running cluster](#running-tests-against-an-already-running-cluster)
section
with a minor difference:

* Specify the correct cluster name and enter the Cloud discovery token like this:

```xml

<hazelcast-client>
    <cluster-name>YOUR_CLUSTER_NAME</cluster-name>

    <network>
        <hazelcast-cloud enabled="true">
            <discovery-token>YOUR_CLUSTER_DISCOVERY_TOKEN</discovery-token>
        </hazelcast-cloud>
    </network>
</hazelcast-client>
``` 

in `client-hazelcast.xml`.

## Controlling the Hazelcast Configuration

You can specify Hazelcast configuration by placing a `hazelcast.xml` (member configuration) or `client-hazelcast.xml` (
client configuration) in your working directory
(the one from which you're executing the `perftest` command). Simulator will handle the upload of them and makes sure
that the
workers are started with them transparently.

If there's no `hazelcast.xml` or `client-hazelcast.xml` in the working directory, Coordinator uses the default
files `${SIMULATOR_HOME}/conf/hazelcast.xml` and `${SIMULATOR_HOME}/conf/client-hazelcast.xml`.

> The recommended approach is to either copy the default XML configurations (listed above) into your working directory
> and
> then modify them, or use the ones generated by `perftest create` as shown in [Quickstart](#quickstart).
> The reason for this is due to the auto-filling markers described below.

### IP addresses and other configuration auto-filling

When you look at the default `hazelcast.xml` or `client-hazelcast.xml` configurations (described above),
you'll probably notice the following comment:

```xml

<hazelcast>
    ...
    <network>
        <join>
            <multicast enabled="false"/>
            <tcp-ip enabled="true">
                <!--MEMBERS-->   
            </tcp-ip>
        </join>
    </network>
</hazelcast>
```

This comment is actually a marker for Simulator where it then automatically places the IP addresses
of the members. Therefore, you don't have to care about it which greatly simplifies the testing.

In general, do not remove this comment or put member IP address manually if you let Simulator handle
the member lifecycle as well (= most of the time, everytime the `--members` is greater than zero).

See [Running tests against an already running cluster](#running-tests-against-an-already-running-cluster) for an
example when editing this section is actually desired.

# Run the test

The actual Simulator Test run is done with the `perftest run` command. The created `tests.yaml` script (
via `perftest create` in [Quickstart](#quickstart))
is a good start to customize your test setup.

## Configure test duration

You can control the duration of the test execution by setting the `duration` property within the `tests.yaml`
definition.
You can specify the time unit for this argument by using

- `s` for seconds
- `m` for minutes
- `h` for hours
- `d` for days

If you omit the time unit the value will be parsed as seconds. The default duration is 60 seconds.

The duration is used as the run phase of a Simulator Test (that's the actual test execution). If you have long running
warmup or
verify phases, the total runtime of the TestSuite will be longer.

> :books: There is another option for the use case where you want to run a Simulator Test until some event occurs (which
> is not time bound),
> e.g. stop after five million operations have been done. In this case, the test code must stop the `TestContext`
> itself. See [Stopping a test](#stopping-a-test) section.

> If you want to run multiple tests in parallel, please refer to
> the [Running multiple tests in parallel](#running-multiple-tests-in-parallel) section.

## Specify TestSuite file to be used

By default `perftest run` will run the `tests.yaml` file in the current directory - you can specify a different file to
use with `perftest run another_file.yaml`.

This is very convenient when you want to test multiple test scenarios on the same cluster setup.

## Installing Simulator on remote machines

Simulator needs to be installed on the remote machines before you run the tests.
If you already have your cloud instances provisioned (
see [Controlling provisioned machines](#controlling-provisioned-machines)) or run a `static` setup (
see [Using static setup](#using-static-setup)), you can just install Hazelcast Simulator with the following command.

```
inventory install simulator
```

This is also useful whenever you update or change your local Simulator installation (e.g. when developing a test
TestSuite)
and want to re-install Hazelcast Simulator on the remote machines.

This is only necessary if the JAR files have been changed. Configuration changes in your `tests.yaml` or
`simulator.properties` **don't require** a new Simulator installation.

# Report generation

Once a benchmark has been executed, if using `perftest run`, an HTML report will automatically be generated for you. You
can
disable this behaviour by passing the `--skip_auto_gen_report` flag to `perftest`. You can always generate an HTML
reprot on
demand by using the `perftest report` tool. Report generation requires Gnuplot 4+ and Python 3.x to be installed for
generating the diagrams.

## Basics

Assume that a benchmark has been executed and the directory `2021-05-31__23_19_13` has been created. To create a report
for that
benchmark, you can use the following command:

```
perftest report -o my-benchmark-report 2021-05-31__23_19_13
```

The name `my-benchmark-report` is output directory's name. The generated report contains detailed throughput and latency
information.
If `dstats` information is available, it shows detailed information about resource utilization such as network, CPU, and
memory.

## Generate comparison reports

The `perftest report` tool is also able to make comparisons between two or more benchmarks.
Suppose that you executed a test with some configuration, the resulting directory is `2021-05-31__23_19_13`. Then you
changed
the configuration, e.g. changed the Hazelcast version and executed again with resulting
directory `2021-05-31__23_35_40`.

You can create a single report plotting those two benchmarks in the same chart allowing easy comparison with:

```
perftest report -o my-comparison-report 2021-05-31__23_19_13 2021-05-31__23_35_40
```

By default the `perftest run` creates a directory for each run inside
`runs/BenchmarkName` with timestamp as the directory name. To automate comparisons of last runs of some benchmarks,
you can simply run the `perftest report` with `-l` flag (or `--last`):

```shell
perftest report -o my-comparison-report runs/MyTestA runs/MyTestB
```

## Extensive reports

You can create a very detailed report with more charts using the `-f` switch:

```
perftest report -f -o my-full-report 2021-05-31__23_19_13 
```

## Warmup and cooldown

It's often desired to strip the beginning or the end of the test out of the resulting charts e.g. because
of JIT compiler warmup etc.

The way it works in Simulator is that the data is collected nevertheless. You just trim it out in the final
report generation with the `perftest report` command. Example having a 1 minute (60 seconds) warmup and 30 second
cooldown:

```
perftest report -w 60 -c 30 -o my-trimmed-benchmark-report 2021-05-31__23_19_13
``` 

# Simulator Properties reference

You can configure Simulator itself using the file `simulator.properties` in your working directory. The default
properties are
always loaded from the `${SIMULATOR_HOME}/conf/simulator.properties` file. Your local properties will override the
defaults.

For the full reference of available settings and their descriptions, please refer
to [default simulator.properties](master/conf/simulator.properties).

# Advanced topics

## Writing a Simulator test

The main part of a Simulator test is writing the actual test. The Simulator test is heavily inspired by the JUnit
testing and
Java Microbenchmark Harness (JMH) frameworks. To demonstrate writing a test, we will start with a very basic case and
progressively add additional features.

For the initial test case we are going to use the `IAtomicLong`. Please see the following snippet:

```java
package example;

...

public class MyTest extends AbstractTest {
    private IAtomicLong counter;

    @Setup public void setup() {
        counter = targetInstance.getAtomicLong("c");
    }

    @TimeStep public void inc() {
        counter.incrementAndGet();
    }
}
```

The above code example shows one of the most basic tests. `AbstractTest` is used to remove duplicate code from tests; so
it
provides access to a logger, `testContext`, `targetInstance` HazelcastInstance, etc.

A Simulator test class needs to be a public, non-abstract class with a public no-arg constructor.

Assume the tests file to start the test is as follows:

```yaml
  test:
    - class: example.MyTest
```

The main property that needs to be in the tests file is the `class` property which needs to point to the full class
name.

Just like the other annotated methods, `Timestep` methods need to be public due to the code generator and they are
allowed to
throw `Throwable` like checked exceptions:

```java
  @TimeStep public void inc() throws Exception {
                counter.incrementAndGet();
                }
```

Any `Throwable`, apart from `StopException`, that is thrown will lead to a Failure being reported.

### Adding properties

Properties can be added to a test to make it easy to modify them from the outside. Properties must be public fields and
can be
primitives, wrappers around primitives like `java.lang.Long`, enums, strings and classes. Properties are case sensitive.

In the below example the `countersLength` property has been added and it defaults to 20.

```java
public class MyTest extends AbstractTest {
    public int countersLength = 20;

    private IAtomicLong[] counters;

    @Setup public void setup() {
        this.counters = new IAtomicLong[countersLength];
        for(int k=0;k<countersLength;k++)
            counters[k] = targetInstance.getAtomicLong(""+k);
    }

    @TimeStep public void inc(BaseThreadState state) {
        int counterIndex = state.randomInt(countersLength);
        counters[counterIndex].incrementAndGet();
    }
}
```

In most cases it is best to provide defaults for properties to make customization of a test less verbose.

The `countersLength` value can be configured as shown below:

```
  test:
  - class: example.MyTest
    countersLength: 1000
```

The order of the properties in the file is irrelevant.

Properties do not need to be simple fields. The property binding supports complex object graphs to be created and
configured.
Properties can be nested and no-arg constructor must be used to build up the graph of objects. Please see the following
example:

```java
public class SomeTest {

    pubic Config config;

    public static class Config {
        NestedConfig nestedConfig;
    }

    public static class NestedConfig {
        public int value;
    }
}
```

The `config` object can be configured as shown below:

```yaml
  test:
    - class: example.SomeTest
      config.nestedConfig.valu: 1000
```

If a property is not used in a test, the test fails during its startup. The reason is that if you would make a typing
error and,
in reality, something different is tested different from what you think is being tested, it is best to know this as soon
as possible.

### ThreadState

A Simulator test instance is shared between all timestep-threads for that test and only on the test instance level where
there
was a state. But in some cases you want to track the state for each timestep-thread. Of course a thread-local variable
can be used
for this, but the Simulator has a more practical and faster mechanism, `ThreadState`.

In the following code example, a `ThreadState` is defined that tracks the number of increments per thread:

```java
import com.hazelcast.Simulator.test.BaseThreadState
...

public class MyTest extends AbstractTest {
    public int countersLength;

    private AtomicLong counter;

    @Setup public void setup() {
        this.counter = targetInstance.getAtomicLong("counter");
    }

    @TimeStep public void inc(ThreadState state) {
        counter.incrementAndGet();
        state.increments++;
    }

    public class ThreadState extends BaseThreadState {
        long increments;
    }
}
```

In this example, tracking the number of increments is not that interesting since nothing is done with it. But it can be
used to
verify that the data structure under the test (`IAtomicLong` in this case) is working correctly. Please see the
[Verification section](#verification) for more information.

The class of the `ThreadState` is determined by timestep code-generator and it will automatically create an instance of
this class
per timestep-thread. This instance will then be passed to each invocation of the timestep method in that
timestep-thread. This
means that you do not need to deal with more expensive thread-locals.

Extending the `BaseThreadState` class is the recommended way to define your own `ThreadState` because it provides
various random
utility methods that are needed frequently.

However, `ThreadState` does not need to extend `BaseThreadState`. `ThreadState` can be any class as long as it has a
no-arg
constructor, or it has a constructor with the type of the enclosing class as argument (a non-static inner
class). `ThreadState`
class unfortunately needs to be a public class due to the code generator. But the internals of the class do not require
any
special treatment.

Another restriction is that all `timestep`, `beforeRun` and `afterRun` methods (of the same execution group) need to
have the
same type for the `ThreadState` argument. So the following is not valid:

```java
public class MyTest extends AbstractTest {

    @TimeStep public void inc(IncThreadState state) {
        counter.incrementAndGet();
        state.increments++;
    }

    @TimeStep public void get(GetThreadState list) {
        counter.get();
    }

    public class IncThreadState { long increments; }
    public class GetThreadState {}
}
```

It is optional for any `timestep`, `beforeRun`, and `afterRun` methods to declare this `ThreadState` argument. So the
following
is valid:

```java
public class MyTest extends AbstractTest {

    @TimeStep public void inc(ThreadState state) {
        counter.incrementAndGet();
        state.increments++;
    }

    @TimeStep public void get() {
        counter.get();
    }

    public class ThreadState extends BaseThreadState {
        long increments;
    }
}
```

The reason for having a single test instance shared between all threads, instead of having a test instance per thread (
and
dropping the need for the `ThreadState`) is that it will be a lot more cache friendly. It is not the test instance which
needs to be put into the cache, but everything referred from the test instance.

Another advantage is that if there is a shared state, it is easier to share it; for example, keys to select from for
a `map.get`
test between threads, instead of each test instance generating its own keys (and therefore increasing memory usage). In
the
future a `@Scope` option will probably be added so that you can choose if each thread gets its own test instance or that
the
test instance is going to be shared.

### AfterRun and BeforeRun

The timestep methods are called by a timestep-thread and each thread will do a loop over its timestep methods. In some
cases
before this loop begins or after this loop ends, some additional logic is required. For example initialization of
the `ThreadState`
object is needed when the loop starts, or updating some shared state when the loop completes. This can be done
using `beforeRun`
and `afterRun` methods. Multiple `beforeRun` and `afterRun` methods can be defined, but the order of their execution is
unfortunately not defined, so be careful with that.

The `beforeRun` and `afterRun` methods accept the `ThreadState` as an argument, but this argument is allowed to be
omitted.

In the following example, `beforeRun` and `afterRun` methods are defined which log when the timestep thread starts, and
log when
it completes. It also writes the number of increments the timestep thread executed:

```java
public class MyTest extends AbstractTest {
    public int countersLength;

    private AtomicLong counter;

    @Setup public void setup() {
        this.counter = targetInstance.getAtomicLong("counter");
    }

    @BeforeRun public void beforeRun(ThreadState state) {
        System.out.println(Thread.currentThread().getName()+" starting");
    }

    @TimeStep public void inc(ThreadState state) {
        counter.incrementAndGet();
        state.increments++;
    }

    @AfterRun public void afterRun(ThreadState state) {
        System.out.println(Thread.currentThread().getName()+
                        " completed with "+state.increments+" increments");
    }

    public class ThreadState extends BaseThreadState {
        long increments;
    }
}
```

### Verification

Once a Simulator test is completed, you can run verifications using the `@Verify` annotation. In the case
of `IAtomicLong.inc`
test, you could count the number of increments per thread. After the test completes, you can verify the total count of
expected
increments and the actual number of increments.

```java
public class MyTest extends AbstractTest {
    private IAtomicLong counter;
    private IAtomicLong expected;

    @Setup public void setup() {
        this.counter = targetInstance.get("counter");
        this.expected = targetInstance.get("expected");
    }

    @TimeStep public void inc(ThreadState state) {
        state.increments++;
        counter.incrementAndGet();
    }

    @AfterRun public void afterRun(ThreadState state) {
        expected.addAndGet(state.increments);
    }

    @Verify public void verify() {
        assertEquals(expected.get(), counter.get())
    }

    public class ThreadState extends BaseThreadState {
        long increments;
    }
}
```

In the above example once the timestep-loop completes, each timestep-thread will call the `afterRun` method and add the
actual
number of increments to the `expected` IAtomicLong object. In the `verify` method the expected number of increments is
compared
with the actual number of increments.

The example also shows we make use of the JUnit's `assertEquals` method. So you can use JUnit or any other framework
that can
verify behaviors. It is even fine to throw an exception.

It is allowed to define zero, one or more verify methods.

By default the verification will run on all workers, but it can be configured to run on a single worker using the global
property on
the `@Verify` annotation.

### TearDown

To automatically remove created resources, a `tearDown` method can be added. It depends on the situation if this is
needed at all for
your test because in most cases the workers will be terminated anyway after the Simulator test completes. But just in
case you
need to tear down the resources, it is possible.

In the following example the `@TearDown` annotation is demonstrated:

```java
public class MyTest extends AbstractTest {
    private IAtomicLong counter;

    @Setup public void setup() {
        counter = targetInstance.getAtomicLong("c");
    }

    @TimeStep public void inc() {
        counter.inc();
    }

    @TearDown public void tearDown() {
        counter.destroy();
    }
}
```

By default the `tearDown` method is executed on all participating workers, but can be influenced using the global
property as shown below:

```java
public class MyTest extends AbstractTest {
    private IAtomicLong counter;

    @Setup public void setup() {
        counter = targetInstance.getAtomicLong("c");
    }

    @TimeStep public void inc() {
        counter.inc();
    }

    @TearDown(global=true) public void tearDown() {
        counter.destroy();
    }
}
```

When `global` is set to `true`, only one worker is going to trigger the `count.destroy()`. It is allowed to define
multiple `tearDown` methods.

### Complete Lifecycle of Calls on the Test

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

### Stopping a Test

By default a Simulator test will run for a given amount of time using the duration property from the `tests.yaml` file.
Please see the following example:

```yaml
- name: my_test_suite
  duration: 300s
```

In this example, all tests in this suite will run for five minutes each. In some cases you need more control over when
to stop. Currently there are
the following options available:

- **Configuring the number of iterations**:
  The number of iterations can be specified using the test properties:

```yaml
    test:
      - class: example.MyTest
        iterations: 1000000
```

In this case the test will run for 1000k iterations.

- **`StopException` to stop a single thread**: When a timestep thread wants to stop, it can throw a `StopException`.
  This
  exception does not lead to a failure of the test. It also has no influence on any other timestep thread.

- **`TestContext.stop` to stop all timestep threads**: All timestep threads for a given period on a single worker can be
  stopped using the `TestContext.stop` method.

In all cases, the Simulator will wait for all timestep threads of all workers to complete. If a duration has been
specified, the
test will not run longer than this duration.

> :books: As the nuclear option, you can use the `inventory destroy` command to destroy your environment if you've got a
> rogue test that won't stop running!

### Code Generation

The timestep methods rely on code generation, that is why a JDK is required to run a timestep based test. The code is
generated
on the fly based on the test and its test parameters. The philosophy is that you should not pay the price for something
that is
not used. For example, if there is a single timestep method, no randomization/switch-case is needed to execute the right
method.
If no logging is configured, no logs are generated.

This way many features can be added to the timestep test without impacting the performance if the actual feature is not
used.

The generator timestep worker code can be found in the worker directory. Feel free to have a look at it and send any
suggestions
how it can be improved.

Currently there is no support for dead code elimination.

## Profiling your Simulator Test

To determine, for example, where the time is spent or other resources are being used, you want to profile your
application.
The recommended way to profile is using the Java Flight Recorder (JFR) which is only available in the Oracle JVMs. The
JFR,
unlike the other commercial profilers like JProbe and Yourkit, does not make use of sampling or instrumentation. It
hooks into
some internal APIs and is quite reliable and causes very little overhead. The problem with most other profilers is that
they
distort the numbers and frequently point you in the wrong direction; especially when I/O or concurrency is involved.
Most of
the recent performance improvements in Hazelcast are based on using JFR.

To enable the JFR, the JVM settings for the member or client need to be modified depending on what needs to be profiled.
Please see the following example:

```
JFR_ARGS="-XX:+UnlockCommercialFeatures  \
          -XX:+FlightRecorder \
          -XX:StartFlightRecording=duration=120m,filename=recording.jfr  \
          -XX:+UnlockDiagnosticVMOptions \
          -XX:+DebugNonSafepoints"
```

If these `JFR_ARGS` are added to the `client_args` and `member_args` properties of the `tests.yaml`, then both client
and members will be configured with JFR. Once the Simulator test has completed, all artifacts including the JFR files
are downloaded. The JFR files can be opened using the Java Mission Control command `jmc`.

## GC analysis

By adding the following options to member/client args, the benchmark generator will do a gc comparison:

```
Java 8:
-Xloggc:gc.log -XX:+PrintGC -XX:+PrintGCDetails -XX:+PrintGCTimeStamps  -XX:+PrintGCDateStamps

Java 9+:
-Xlog:gc:file=gc.log:utctime,pid,tags:filecount=32,filesize=64m
```

## Reducing Fluctuations

For more stable performance numbers, set the minimum and maximum heap size to the same value, i.e. `-Xms2G -Xmx2G`

Also set the minimum cluster size to the expected number of members using the following property:

```
-Dhazelcast.initial.min.cluster.size=4
```

This prevents the Hazelcast cluster from starting before the minimum number of members has been reached. Otherwise, the
benchmark
numbers of the tests can be distorted due to partition migrations during the test. Especially with a large number of
partitions
and short tests, this can lead to a very big impact on the benchmark numbers.

## Enabling Diagnostics

Hazelcast has a diagnostics system which provides detailed insights on what is happening inside the client or server
`HazelcastInstance`. It is designed to run in production and has very little performance overhead. It has so little
overhead
that we always enable it when doing benchmarks.

```yaml

members_args: "-Dhazelcast.diagnostics.enabled=true \
                               -Dhazelcast.diagnostics.metric.level=info \
                               -Dhazelcast.diagnostics.invocation.sample.period.seconds=30 \
                               -Dhazelcast.diagnostics.pending.invocations.period.seconds=30 \
                               -Dhazelcast.diagnostics.slowoperations.period.seconds=30" \

client_args: "-Dhazelcast.diagnostics.enabled=true \
                               -Dhazelcast.diagnostics.metric.level=info" \

```

If these flags are added to the `client_args` and `member_args` respectively, both client and server will have
diagnostics enabled. Both will write a diagnostics file. Once the Simulator
run is completed and the artifacts are downloaded, the diagnostics files can be analyzed.

## Logging

In some cases, especially when debugging, logging is required. One easy way to add logging is to add logging into the
timestep
method. But this can be inefficient and it is frequently noisy. Using some magic properties logging can be enabled on
any timestep
based Simulator test. There are two types of logging:

- **frequency based**; for example every 1000th iteration, each timestep thread will log where it is.
- **time rate based**; for example every 100ms each timestep thread will log where it is. Time rate based is quite
  practical
  because you do not get swamped or a shortage of log entries, like the frequency based one.

You can configure frequency based logging as shown below:

```yaml
  test:
    - class: example.MyTest
      logFrequency: 10000
```

In this example, every 10000 iteration, a log entry is made per timestep thread.

You can configure time rate based logging as shown below:

```yaml
  test:
    - class: example.MyTest
      logRateMs: 100
```

In this example, at most every 100ms, a log entry is made per timestep thread.

## Running multiple tests in parallel

It's possible to run multiple tests simultaneously. In order to do that, the `tests.yaml` needs to be setup similarly to
this:

  ```yaml
  - name: parallel_test
    repetitions: 1
    duration: 300s
    clients: 1
    members: 1
    loadgenerator_hosts: loadgenerators
    node_hosts: nodes
    driver: hazelcast5
    version: maven=5.1
    client_args: >
      -Xms3g
      -Xmx3g
    member_args: >
      -Xms3g
      -Xmx3g
    parallel: True
    test:
      - class: com.hazelcast.simulator.tests.map.MapCasTest
        threadCount: 3
        keyCount: 1000
      - class: com.hazelcast.simulator.tests.map.MapLockTest
        name: MapLock_1k_keys
        threadCount: 3
        keyCount: 1000
      - class: com.hazelcast.simulator.tests.map.MapLockTest
        name: MapLock_5k_keys
        threadCount: 3
        keyCount: 5000
  ```

The key aspects of this configuration that allows running multiple tests in parallel are:

* `parallel: True` needs to be set in the test suite properties - otherwise all tests are run serially.
* Multiple `test` entries need to be defined; you can't run 1 test in parallel!
* `test` entries that share the same test `class` should have unique `name` properties defined per test - otherwise only
  1 of the tests will be run (as the `class` is used for the test name if not defined explicitly).

## Controlling the load generation

Besides the cluster layout you can also control the number of Workers which will execute their RUN phase (= the actual
test).
The default is that client Workers are preferred over member Workers. That means if client Workers are used, they will
create the load in the cluster,
otherwise the member Workers will be used.

```
perftest exec --targetCount 2
```

This will limit the load generation to two Workers, regardless of the load generator Workers' availability. Please have
a look
at command line help via `perftest exec --help` to see all allowed values for these arguments.

## Various forms of testing

performance_monitor_interval_seconds: 1

### Throughput testing

A throughput test is a test where the throughput is measured at some level of concurrency.

For example:

  ```yaml
  test:
    - class: com.hazelcast.simulator.tests.map.IntByteMapTest
      name: MyByteTest
      threadCount: 40
      getProb: 1
      putProb: 0
      keyCount: 1_000_000
  ```

Each load generator (e.g. client) will run with 40 threads and call the IMap.get method.

This is the most common form of throughput testing but isn't ideal. The ideal form of throughput testing would
be to determine the maximum possible throughput of a system by increasing the load.

### Latency testing

Simulator can also be used to run a latency test. It will determine the latency for a fixed throughput:

  ```yaml
  test:
    - class: com.hazelcast.simulator.tests.map.IntByteMapTest
      name: MyByteTest
      threadCount: 40
      getProb: 1
      putProb: 0
      keyCount: 1_000_000
      ratePerSecond: 100
  ```

With 1 client, there would be 100 requests per second. With 2 clients, there would be 200 requests per second.
Simulator will handle coordinated omission correctly.

### Stress testing

With a stress test the load is increased until the system collapses. This can be done using the
`rampupSeconds`. This is demonstrated in the following example:

  ```yaml
  test:
    - class: com.hazelcast.simulator.tests.map.IntByteMapTest
      name: MyByteTest
      threadCount: 400
      getProb: 1
      putProb: 0
      keyCount: 1_000_000
      ratePerSecond: 100
      rampupSeconds: 400
  ```

The `rampupSeconds` is configured as 400; which means that every second 1 thread is going to start.
This will happen at every load generator (e.g. client).

### Soak testing

A soak test determines how the system behaves over a long period of time, for example to check 
if there any memory leaks. The only thing that needs to be done for this is to configure the test
duration with a high value e.g. 12h. Example:

  ```yaml
        - name: read_only
          repetitions: 1
          duration: 24h
          clients: 1
          members: 1
          loadgenerator_hosts: loadgenerators
          node_hosts: nodes
          driver: hazelcast5
          version: maven=5.1
          test:
            - class: com.hazelcast.simulator.tests.map.IntByteMapTest
              name: MyByteTest
              threadCount: 40
              getProb: 1
              putProb: 0
              keyCount: 1_000_000
   ```

### Volume testing

A volume test determines if a system can handle large volumes of data. The simplest approach would
be to set the `keyCount` to a large value (it depends on the test of this property is available):

  ```yaml
        test:
          - class: com.hazelcast.simulator.tests.map.LongByteArrayMapTest
            name: LongByteArrayMapTest
            threadCount: 400
            getProb: 1
            putProb: 0
            keyDomain: 100_000_000
            ratePerSecond: 100
            minValueLength: 1_000
            maxValueLength: 1_000
   ```

This will load 100M x 1KB = 100 GB of data into the cluster.

### Scalability testing

A scalability test determines how well a system scales when nodes are added. The simplest way to
do a scalability test is to pick some performance test like a throughput test and run it. E.g.

  ```yaml
        - name: read_only
          repetitions: 1
          duration: 300s
          clients: 1
          members: 1
          loadgenerator_hosts: loadgenerators
          node_hosts: nodes
          driver: hazelcast5
          version: maven=5.1
          test:
            - class: com.hazelcast.simulator.tests.map.IntByteMapTest
              name: MyByteTest
              threadCount: 40
              getProb: 1
              putProb: 0
              keyCount: 1_000_000
   ```

After completion, increase members and run it again. Make sure that sufficient node machines are available.


## Network constraints

When testing the throughput, results are constrained by factors including CPU, memory, network, and/or a combination of those.
Therefore, it's crucial to know these constraints and analyse the test results in their context.

Cloud providers specify the availability of CPU and memory for different instance types,
howeveer  they are much less verbose on network-related limits.

There are two main limitations in play related to network resources: bandwidth (bits/s) and packet count (packets/s).
Hazelcast-simulator contains a tool that allows measuring the limits of bandwidth and packet count, based on `iperf3`.

In order to use the tool, you first need to install the simulator and iperf3 on the machines.

```shell
inventory install simulator
inventory install iperf3
```

Finally, you can run the pps benchmark:

```shell
iperf3test pps <server> <client>
```

where `<server>` and `<client>` are public IP addresses of the instances between which you want to measure max PPS.

The test generates high-PPS traffic from server to client and from client to server.
The actual PPS stats are recorded on the server side and reported in the terminal output.

When running the tool on AWS instances, the output also includes
the information about the number of `pps_allowance_exceeded` events recorded every second.
High number of `pps_allowance_exceeded` events strongly suggests that the test has in fact run into the pps limit.

In case the two instances have a different PPS limit,
the PPS of the connection between them is generally constrained by the smaller one.
If you're running the test with the instance with bigger limit as the server,
the actual PPS might be limited by the client-side limits.
In such case, the test might run into the PPS limit of the connection,
but on the server side `pps_allowance_exceeded` might show 0 events/s.

For any pair of instances A and B, it is advised to run the PPS test for both A and B as the server.
This ensure a clear picture of all the PPS limits across instances.


# Get Help

You can use the following channels for getting help with Hazelcast:

* [Hazelcast mailing list](http://groups.google.com/group/hazelcast)
* [Slack](https://slack.hazelcast.com/) for chatting with the
  development team and other Hazelcast users.
* [Stack Overflow](https://stackoverflow.com/tags/hazelcast)
