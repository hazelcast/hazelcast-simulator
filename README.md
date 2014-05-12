hazelcast-stabilizer
===========================

A tool for stress testing Hazelcast and Hazelcast based applications in clustered environments. This can be in a local
machine, but can also be in a cloud like EC2 or Google Compute Engine. The Stabilizer makes use of JClouds, so in theory
we can roll out in any cloud.

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

* Provisioner: responsible for spawning/terminating EC2 instances and to install Agents on remote machines. It can be used
in combination with EC2 (or any other cloud), but it can also be used in a static setup like a local machine or the
test cluster we have in Istanbul office.

#### Installation

The zip/tar.gz file containing the stabilizer artifacts can be downloaded here:

```
https://oss.sonatype.org/content/repositories/snapshots/com/hazelcast/stabilizer/hazelcast-stabilizer-dist/0.3-SNAPSHOT/
```

Download and unpack the tar.gz or zip file to e.g. the home directory.

add to ~/.bashr:

```
export STABILIZER_HOME=~/hazelcast-stabilizer-0.3-SNAPSHOT
PATH=$STABILIZER_HOME/bin:$PATH
```

Create your tests working directory, e.g.

```
mkdir ~/tests
```

Copy the STABILIZER_HOME/conf/stabilizer.properties to the tests directory. And make
the changes required. In case of EC2, you only need to make the following changes:

```
CLOUD_IDENTITY=<your-aws-access-key>
CLOUD_CREDENTIAL=<your-aws-secret-key>
```

### Setup public key

After you have set up stabilizer, make sure you have a id_rsa.pub in your ~/.ssh directory. If not, one can be generated
like this:

```
ssh-keygen -t rsa -C "your_email@example.com"
```

You can press enter on all questions. The value for the email address is not relevant. After this command has completed, you
should have a ida_rsa.pub and id_rsa file in your ~/.ssh directory. Your id_rsa.pub key will automatically be copied to
the remote agent machines and added to the ~/.ssh/known_hosts file, so that you can log into that machine without
a password or explicit provided credentials.

### Using the archetype

Probably you want to write your own test. The easiest way to do that is to make use of the Stabilizer archetype which
will generate a project for you.

```
mvn archetype:generate \
    -DarchetypeRepository=https://oss.sonatype.org/content/repositories/snapshots \
    -DarchetypeGroupId=com.hazelcast.stabilizer \
    -DarchetypeArtifactId=archetype \
    -DarchetypeVersion=0.3-SNAPSHOT \
    -DgroupId=yourgroupid  \
    -DartifactId=yourproject
```

After this project is generated, go to the created directory and run:

```
mvn clean install
```

And then go to workingdir and edit the stabilizer.properties file. In case of EC2, you only need to alter the following:

```
CLOUD_IDENTITY=<your-aws-access-key>
CLOUD_CREDENTIAL=<your-aws-secret-key>
```

After you have made the modifications, you need to set the executable flag on the run.sh:

```
chmod +x run.sh
```

This is needed because the maven archetype is not able to deal with the executable flag.

And finally you can run the test:

```
./run.sh
```

This script will:
 * start 4 EC2 instances, install Java, install the agents.
 * upload your jars, run the test using a 2 node test cluster and 2 client machines (the clients generate the load). This
   test will run for 2 minutes.
 * After the test completes the the artifacts (log files) are downloaded in the 'workers' directory
 * terminate the 4 created instances. If you don't want to start/terminate the instances for every run, just comment out
   'provisioner --terminate' line.  This prevents the machines from being terminated.

The output will look something like this:

```
INFO  12:58:24 Hazelcast Stabilizer Provisioner
INFO  12:58:24 Version: 0.3-SNAPSHOT
INFO  12:58:24 STABILIZER_HOME: /home/alarmnummer/hazelcast-stabilizer-0.3-SNAPSHOT
INFO  12:58:24 Loading stabilizer.properties: /java/projects/Hazelcast/example/yourproject/workingdir/stabilizer.properties
INFO  12:58:24 ==============================================================
INFO  12:58:24 Provisioning 4 aws-ec2 machines
INFO  12:58:24 ==============================================================
INFO  12:58:24 Current number of machines: 0
INFO  12:58:24 Desired number of machines: 4
INFO  12:58:24 GroupName: stabilizer-agent
INFO  12:58:24 JDK spec: oracle 7
INFO  12:58:24 Hazelcast version-spec: outofthebox
INFO  12:58:26 Created compute
INFO  12:58:26 Machine spec: hardwareId=m3.medium,imageId=us-east-1/ami-fb8e9292
INFO  12:58:32 Security group: 'stabilizer' is found in region 'us-east-1'
INFO  12:58:40 Created template
INFO  12:58:40 Loginname to the remote machines: stabilizer
INFO  12:58:40 Creating nodes
INFO  12:58:40 Created machines, waiting for startup (can take a few minutes)
INFO  13:00:10 	75.101.216.36 LAUNCHED
INFO  13:00:10 	54.80.136.253 LAUNCHED
INFO  13:00:10 	54.82.114.29 LAUNCHED
INFO  13:00:10 	54.87.117.225 LAUNCHED
INFO  13:00:25 	75.101.216.36 JAVA INSTALLED
INFO  13:00:25 	54.82.114.29 JAVA INSTALLED
INFO  13:00:25 	54.80.136.253 JAVA INSTALLED
INFO  13:00:27 	54.87.117.225 JAVA INSTALLED
INFO  13:02:46 	54.82.114.29 STABILIZER AGENT INSTALLED
INFO  13:02:49 	54.82.114.29 STABILIZER AGENT STARTED
INFO  13:02:54 	75.101.216.36 STABILIZER AGENT INSTALLED
INFO  13:02:57 	75.101.216.36 STABILIZER AGENT STARTED
INFO  13:02:58 	54.80.136.253 STABILIZER AGENT INSTALLED
INFO  13:03:00 	54.87.117.225 STABILIZER AGENT INSTALLED
INFO  13:03:01 	54.80.136.253 STABILIZER AGENT STARTED
INFO  13:03:03 	54.87.117.225 STABILIZER AGENT STARTED
INFO  13:03:03 Duration: 00d 00h 04m 39s
INFO  13:03:03 ==============================================================
INFO  13:03:03 Successfully provisioned 4 aws-ec2 machines
INFO  13:03:03 ==============================================================
INFO  13:03:03 Hazelcast Stabilizer Coordinator
INFO  13:03:03 Version: 0.3-SNAPSHOT
INFO  13:03:03 STABILIZER_HOME: /home/alarmnummer/hazelcast-stabilizer-0.3-SNAPSHOT
INFO  13:03:04 Loading stabilizer.properties: /java/projects/Hazelcast/example/yourproject/workingdir/stabilizer.properties
INFO  13:03:04 Using agents file: /java/projects/Hazelcast/example/yourproject/workingdir/agents.txt
INFO  13:03:04 Waiting for agents to start
INFO  13:03:04 Connect to agent 75.101.216.36 OK
INFO  13:03:04 Connect to agent 54.80.136.253 OK
INFO  13:03:05 Connect to agent 54.82.114.29 OK
INFO  13:03:05 Connect to agent 54.87.117.225 OK
INFO  13:03:05 Performance monitor enabled: false
INFO  13:03:05 Total number of agents: 4
INFO  13:03:05 Total number of Hazelcast member workers: 2
INFO  13:03:05 Total number of Hazelcast client workers: 2
INFO  13:03:05 Total number of Hazelcast mixed client & member workers: 0
INFO  13:03:22 Finished starting a grand total of 4 Workers JVM's after 16413 ms
INFO  13:03:22 Starting testsuite: 1399802584004
INFO  13:03:23 Tests in testsuite: 1
INFO  13:03:23 Running time per test: 00d 00h 05m 00s 
INFO  13:03:24 Expected total testsuite time: 00d 00h 05m 00s
INFO  13:03:24 Running Test : 1399802584003
TestCase{
      id=1399802584003
    , class=com.yourgroupid.ExampleTest
    , logFrequency=10000
    , performanceUpdateFrequency=10000
    , threadCount=1
}
INFO  13:03:24 Starting Test initialization
INFO  13:03:27 Completed Test initialization
INFO  13:03:27 Starting Test local setup
INFO  13:03:29 Completed Test local setup
INFO  13:03:29 Starting Test global setup
INFO  13:03:31 Completed Test global setup
INFO  13:03:31 Starting Test start
INFO  13:03:33 Completed Test start
INFO  13:03:33 Test will run for 00d 00h 05m 00s
INFO  13:04:03 Running 00d 00h 00m 30s, 10.00 percent complete
INFO  13:04:33 Running 00d 00h 01m 00s, 20.00 percent complete
INFO  13:05:03 Running 00d 00h 01m 30s, 30.00 percent complete
INFO  13:05:33 Running 00d 00h 02m 00s, 40.00 percent complete
INFO  13:06:03 Running 00d 00h 02m 30s, 50.00 percent complete
INFO  13:06:33 Running 00d 00h 03m 00s, 60.00 percent complete
INFO  13:07:03 Running 00d 00h 03m 30s, 70.00 percent complete
INFO  13:07:33 Running 00d 00h 04m 00s, 80.00 percent complete
INFO  13:08:03 Running 00d 00h 04m 30s, 90.00 percent complete
INFO  13:08:33 Running 00d 00h 05m 00s, 100.00 percent complete
INFO  13:08:34 Test finished running
INFO  13:08:34 Starting Test stop
INFO  13:08:35 Completed Test stop
INFO  13:08:36 Starting Test global verify
INFO  13:08:37 Completed Test global verify
INFO  13:08:37 Starting Test local verify
INFO  13:08:39 Completed Test local verify
INFO  13:08:39 Starting Test global tear down
INFO  13:08:41 Finished Test global tear down
INFO  13:08:41 Starting Test local tear down
INFO  13:08:43 Completed Test local tear down
INFO  13:08:43 Terminating workers
INFO  13:08:45 All workers have been terminated
INFO  13:08:45 Starting cool down (20 sec)
INFO  13:09:05 Finished cool down
INFO  13:09:05 Total running time: 342 seconds
INFO  13:09:05 -----------------------------------------------------------------------------
INFO  13:09:05 No failures have been detected!
INFO  13:09:05 -----------------------------------------------------------------------------
INFO  13:09:05 Hazelcast Stabilizer Provisioner
INFO  13:09:05 Version: 0.3-SNAPSHOT
INFO  13:09:05 STABILIZER_HOME: /home/alarmnummer/hazelcast-stabilizer-0.3-SNAPSHOT
INFO  13:09:05 Loading stabilizer.properties: /java/projects/Hazelcast/example/yourproject/workingdir/stabilizer.properties
INFO  13:09:05 ==============================================================
INFO  13:09:05 Download artifacts of 4 machines
INFO  13:09:05 ==============================================================
INFO  13:09:05 Downloading from 75.101.216.36
INFO  13:09:08 Downloading from 54.80.136.253
INFO  13:09:10 Downloading from 54.82.114.29
INFO  13:09:13 Downloading from 54.87.117.225
INFO  13:09:15 ==============================================================
INFO  13:09:15 Finished Downloading Artifacts of 4 machines
INFO  13:09:15 ==============================================================
INFO  13:09:16 Hazelcast Stabilizer Provisioner
INFO  13:09:16 Version: 0.3-SNAPSHOT
INFO  13:09:16 STABILIZER_HOME: /home/alarmnummer/hazelcast-stabilizer-0.3-SNAPSHOT
INFO  13:09:16 Loading stabilizer.properties: /java/projects/Hazelcast/example/yourproject/workingdir/stabilizer.properties
INFO  13:09:16 ==============================================================
INFO  13:09:16 Terminating 4 aws-ec2 machines (can take some time)
INFO  13:09:16 ==============================================================
INFO  13:09:16 Current number of machines: 4
INFO  13:09:16 Desired number of machines: 0
INFO  13:09:53 	75.101.216.36 Terminating
INFO  13:09:53 	54.82.114.29 Terminating
INFO  13:09:53 	54.80.136.253 Terminating
INFO  13:09:53 	54.87.117.225 Terminating
INFO  13:10:38 Updating /java/projects/Hazelcast/example/yourproject/workingdir/agents.txt
INFO  13:10:38 Duration: 00d 00h 01m 21s
INFO  13:10:38 ==============================================================
INFO  13:10:38 Finished terminating 4 aws-ec2 machines, 0 machines remaining.
INFO  13:10:38 ==============================================================

```

#### Provisioning

The behavior of the cluster like cloud, os, hardware, jvm version, Hazelcast version or region can be configured through the
stabilizer.properties.

To start a cluster:

```
provisioner --scale 1
```

To scale to 2 member cluster:

```
provisioner --scale 2
```

To scale back 1 member:

```
provisioner --scale 1
```

To terminate all members in the cluster

```
provisioner --terminate
```

or

```
provisioner --scale 0
```

If you want to restart all agents and also reupload the newest jars to the machines:

```
provisioner --restart
```

To download all worker home directories (containing logs and whatever has been put inside)

```
provisioner --download
```

To remove all the worker home directories

```
provisioner --clean
```

#### Controlling Deployment

Deploying a test on the agent machines is as simple as:

```
coordinator yourtest.properties.
```

This will create a single worker per agent and run the test for 60 seconds.

### Accessing the provisioned machine

When a machine is provisioned, by default a user with the name 'stabilizer' is create on the remote machine and added
to the sudoers list. Also the public key of your local user is copied to the remote machine and added to
~/.ssh/authorized_keys. So you can login to that machine using:

```
ssh stabilizer@ip
```

You can change name of the created user to something else in by setting the "USER=somename" property in the stabilizer
properties. Be careful not to pick a name that is used on the target image. E.g. if you use ec2-user/ubuntu, and the
default user of that image is ec2-user/ubuntu, then you can run into authentication problems. So probably it is best
not to change this value, unless you know what your are doing.

## Controlling the Hazelcast xml configuration

By default the coordinator makes use of STABILIZER_HOME/conf/hazelcast.xml and STABILIZER_HOME/conf/client-hazelcast.xml
to generate the correct Hazelcast configuration. But you can override this, so you can use your own configuration:

coordinator --clientHzFile=your-client-hazelcast.xml --hzFile your-hazelcast.xml ....

## Controlling duration:
The duration of a single test can be controlled using the --duration setting, which defaults to 60 seconds.

```
coordinator --duration 90s  map.properties
```

```
coordinator --duration 3m  map.properties
```

```
coordinator --duration 12h  map.properties
```

```
coordinator --duration 2d  map.properties
```

## Controlling client/workers:

By default the provisioner will only start members, but you can control how many clients you want to have.

```
coordinator --memberWorkerCount 4 --clientWorkerCount 8 --duration 12h  map.properties
```

In this case we create a 4 node Hazelcast cluster and 8 clients and all load will be generated through the clients. We run
the map.properties test for a duration of 12 hours. Also m for minutes, d for days or s for seconds can be used.

One of the suggestions is that currently the profiles are configured with X clients and Y servers.

But it could be that you only want to have servers and no clients:

```
coordinator --memberWorkerCount 12  --duration 12h  map.properties
```

Or maybe you want to have a JVM with embedded client + server but all communication goes through the client:

```
coordinator --mixedWorkerCount 12  --duration 12h  map.properties
```

Or maybe you want to run 2 member JVM's per machine:

```
coordinator --memberWorkerCount 24  --duration 12h  map.properties
```

You can very easily play with the actual deployment.

### Creating your own Test.

The simplest option, unless you are a Hazelcast committer, to add your own test is to make use of the archetype, e.g.

```
mvn archetype:generate -DarchetypeGroupId=com.hazelcast.stabilizer \
                        -DarchetypeArtifactId=archetype \
                        -DarchetypeVersion=0.3-SNAPSHOT \
                        -DgroupId=yourcompany \
                        -DartifactId=yourproject
```

### Mail Group

Please join the mail group if you are interested in using or developing Hazelcast.

[http://groups.google.com/group/hazelcast](http://groups.google.com/group/hazelcast)

#### License

Hazelcast Stabilizer is available under the Apache 2 License.

#### Copyright

Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.

Visit [www.hazelcast.com](http://www.hazelcast.com/) for more info.