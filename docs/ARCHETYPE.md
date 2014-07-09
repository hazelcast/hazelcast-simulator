### Using the archetype

Probably you want to write your own test. The easiest way to do that is to make use of the Stabilizer archetype which
will generate a project for you.

```
mvn archetype:generate -DarchetypeRepository=https://oss.sonatype.org/content/repositories/snapshots \
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

And then go to workdir and edit the stabilizer.properties file. In case of EC2, you only need to alter the following:

```
CLOUD_IDENTITY=~/ec2.identity
CLOUD_CREDENTIAL=~/ec2.credential
```

The ec2.identity file should contain your access key and the ec2.credential your secret key.

And finally you can run the test from the workdir directory:

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
INFO  12:58:24 Loading stabilizer.properties: /java/projects/Hazelcast/example/yourproject/workdir/stabilizer.properties
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
INFO  13:03:04 Loading stabilizer.properties: /java/projects/Hazelcast/example/yourproject/workdir/stabilizer.properties
INFO  13:03:04 Using agents file: /java/projects/Hazelcast/example/yourproject/workdir/agents.txt
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
INFO  13:09:05 Loading stabilizer.properties: /java/projects/Hazelcast/example/yourproject/workdir/stabilizer.properties
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
INFO  13:09:16 Loading stabilizer.properties: /java/projects/Hazelcast/example/yourproject/workdir/stabilizer.properties
INFO  13:09:16 ==============================================================
INFO  13:09:16 Terminating 4 aws-ec2 machines (can take some time)
INFO  13:09:16 ==============================================================
INFO  13:09:16 Current number of machines: 4
INFO  13:09:16 Desired number of machines: 0
INFO  13:09:53 	75.101.216.36 Terminating
INFO  13:09:53 	54.82.114.29 Terminating
INFO  13:09:53 	54.80.136.253 Terminating
INFO  13:09:53 	54.87.117.225 Terminating
INFO  13:10:38 Updating /java/projects/Hazelcast/example/yourproject/workdir/agents.txt
INFO  13:10:38 Duration: 00d 00h 01m 21s
INFO  13:10:38 ==============================================================
INFO  13:10:38 Finished terminating 4 aws-ec2 machines, 0 machines remaining.
INFO  13:10:38 ==============================================================

```