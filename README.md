# Table of Contents

* [Hazelcast Simulator](#hazelcast-simulator)
* [Key Concepts](#key-concepts)
* [Installing Simulator](#installing-simulator)


# Hazelcast Simulator

Hazelcast Simulator is a production simulator used to test Hazelcast and Hazelcast-based applications in clustered environments. It also allows you to create your own tests and perform them on your Hazelcast clusters and applications that are deployed to cloud computing environments. In your tests, you can provide any property that can be specified on these environments (Amazon EC2, Google Compute Engine(GCE), or your own environment): properties such as hardware specifications, operating system, Java version, etc.

Hazelcast Simulator allows you to add potential production problems, such as real-life failures, network problems, overloaded CPU, and failing nodes to your tests. It also provides a benchmarking and performance testing platform by supporting performance tracking and also supporting various out-of-the-box profilers.

Hazelcast Simulator makes use of Apache jclouds&reg;, an open source multi-cloud toolkit that is primarily designed for testing on the clouds like Amazon EC2 and GCE.

You can use Hazelcast Simulator for the following use cases:

- In your pre-production phase to simulate the expected throughput/latency of Hazelcast with your specific requirements.
- To test if Hazelcast behaves as expected when you implement a new functionality in your project.
- As part of your test suite in your deployment process.
- When you upgrade your Hazelcast version.

Hazelcast Simulator is available as a downloadable package on the Hazelcast <a href="http://www.hazelcast.org/download" target="_blank">web site</a>. Please refer to the [Installing Simulator section](#installing-simulator) for more information.

Simulator includes a test suite for our own stress simulation, but you can fork this repo, and add your own.

Commercially we offer support agreements where we will integrate your tests into our runs for new releases so that your
tests act as an Application TCK. 

# Key Concepts

The following are the key concepts mentioned with Hazelcast Simulator.

- **Test** - A test class for the functionality you want to test, e.g. a Hazelcast map. This test class looks similar to a JUnit test, but it uses custom annotations to define methods for different test phases (e.g. `@Setup`, `@Warmup`, `@Run`, `@Verify`).

- **TestSuite** - A property file that contains the name of the `Test` class and the properties you want to set on that `Test` class instance. A `TestSuite` contains one or multiple tests. It can also contain the same `Test` class with different names and configurations.

- **Worker** - This term `Worker` is used twice in Simulator. 

  - **Simulator Worker** - A Java Virtual Machine (JVM) responsible for running the configured `Tests`. It can be configured to spawn a Hazelcast client or member instance, which is used in the tests. We refer to this `Worker` in the context of a Simulator component like `Agent` and `Coordinator`.
  
  - **Test Worker** - A Runnable implementation to increase the test workload by spawning several threads in each `Test` instance. We refer to this `Worker` in the context of a `Test`, e.g. how many worker threads a `Test` should create.

- **Agent** - A JVM responsible for managing client and member `Workers`. There is always one `Agent` per physical machine, no matter how many `Workers` are spawned on that machine. It serves as communication relay for the `Coordinator` and monitoring instance for the `Workers`.

- **Coordinator** - A JVM that can run anywhere, such as on your local machine. The `Coordinator` is actually responsible for running the `TestSuite` using the `Agents` and `Workers`. You configure it with a list of `Agent` IP addresses, and you run it by executing a command like "run this testsuite with 10 member worker and 100 client worker JVMs for 2 hours".

- **Coordinator Remote** - A JVM that can run anywhere, such as on your local machine. The `CoordinatorRemote` is responsible for sending instructions to the Coordinator. For basic simulator usages the remote is not needed, but for complex scenarios such as **rolling upgrade** or **high availability** testing, a much more interactive approach is required. The coordinator remote talks to the coordinator using TCP/IP.

- **Provisioner** - Spawns and terminates cloud instances, and installs Hazelcast Simulator on the remote machines. It can be used in combination with EC2 (or any other cloud), but it can also be used in a static setup, such as a local machine or a cluster of machines in your data center.

- **Failure** - An indication that something has gone wrong. Failures are picked up by the `Agent` and sent back to the `Coordinator`.

- **simulator.properties** - The configuration file you use to adapt the Hazelcast Simulator to your business needs (e.g. cloud provider, SSH username, Hazelcast version, Java profiler settings).

# Installing Simulator

Hazelcast Simulator needs a Unix shell to run. Ensure that your local and remote machines are running under Unix, Linux or Mac OS. Hazelcast Simulator may work with Windows using a Unix-like environment such as Cygwin, but that is not officially supported at the moment.

## Setting up the Local Machine

The local machine will be the one on which you will eventually execute the Coordinator to run your TestSuite. It is also the source to install Simulator on your remote machines.

Hazelcast Simulator is provided as a separate downloadable package, in `zip` or `tar.gz` format. You can download either one [here](http://hazelcast.org/download/#simulator).

After the download is completed, follow the below steps.

- Unpack the `tar.gz` or `zip` file to a folder that you prefer to be the home folder for Hazelcast Simulator. The file extracts with the name `hazelcast-simulator-<`*version*`>`. If your are updating Simulator you are done and can skip the following steps.

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

- Open a new terminal to make your changes in `~/.bashrc` or `~/.profile` effective. Call the Simulator Wizard with the `--help` option to see if your installation was successful.

  ```
  simulator-wizard --help
  ```

## First Run of Simulator

After the installation you can already use Simulator on the local machine.

- Create a working directory for your Simulator TestSuite. Use the Simulator Wizard to create an example setup for you and change to that directory.

  ```
  simulator-wizard --createWorkDir tests
  cd tests
  ```
  
- Execute the created `run` script to run the TestSuite.

  ```
  ./run
  ```

Congratulations, you successfully ran Simulator for the first time! Please refer to the [Customizing your Simulator Setup section](#customizing-your-simulator-setup) to learn how to configure your test setup.

## Preparations to setup Remote Machines

Beside the local setup, there are also static setups (fixed list of given remote machines, e.g. your local machines, a test laboratory) and cloud setups (e.g. Amazon EC2, Google Compute Engine). For all those remote machines, you need to configure a password free SSH access. You may also need to configure the firewall between your local and the remote machines.

## Firewall Settings

Please ensure that all remote machines are reachable via TCP ports 22, 9000 and 5701 to 57xx on their external network interface (e.g. `eth0`). The first two ports are used by Hazelcast Simulator. The other ports are used by Hazelcast itself. Ports 9001 to 90xx are used on the loopback device on all remote machines for local communication.

![](images/Network.png)

- Port 22 is used for SSH connections to install Simulator on remote machines, to start the Agent and to download test result and log files. If you use any other port for SSH, you can configure Simulator to use it via the `SSH_OPTIONS` property in the `simulator.properties` file.
- Port 9000 is used for the communication between Coordinator and Agent. You can configure this port via the `AGENT_PORT` property in the `simulator.properties` file.
- Ports 9001 to 90xx are used for the communication between Agent and Worker. We use as many ports as Worker JVMs are spawned on the machine.
- Ports 5701 to 57xx are used for the Hazelcast instances to form a cluster. We use as many ports as Worker JVMs are spawned on the machine, since each of them will create its own Hazelcast instance.

## Creating an RSA key pair

The preferred method for password free authentication is using an RSA (Rivest, Shamir and Adleman crypto-system) public/private key pair. The RSA key should not require you to enter the pass-phrase manually. A key with a pass-phrase and ssh-agent-forwarding is strongly recommended, but a key without a pass-phrase also works.

If you already have an RSA key pair, you willl find the files `id_rsa.pub` and `id_rsa` in your local `~/.ssh` folder. If you do not have RSA keys, you can generate a public/private key pair using the following command.

```
ssh-keygen -t rsa -C "your-email@example.com"
```

Press `[Enter]` for all questions. The value for the e-mail address is not relevant in this case. After you execute this command, you should have the files `id_rsa.pub` and `id_rsa` in your `~/.ssh` folder.



# Mail Group

Please join the mail group if you are interested in using or developing Hazelcast.

[http://groups.google.com/group/hazelcast](http://groups.google.com/group/hazelcast)

# License

Hazelcast Simulator is available under the Apache 2 License.

# Copyright

Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.

Visit [www.hazelcast.com](http://www.hazelcast.com/) for more info. Also, see the Hazelcast Simulator chapter in the Reference Manual at [http://hazelcast.org/documentation/](http://hazelcast.org/documentation/). Docs for latest version can be found [here](http://docs.hazelcast.org/docs/latest/manual/html-single/index.html#hazelcast-simulator)
