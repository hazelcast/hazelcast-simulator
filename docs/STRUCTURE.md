General structure
===========================

* Test: the functionality you want to test, e.g. a map. It looks a bit like a JUnit test, but it doesn't use annotations
and has a bunch of methods that one can override.

* TestSuite: this is a property file that contains the name of the Test class and the properties you want to set on that
test class instance. In most cases a testsuite contains a single test class, but you can configure multiple tests within
a single testsuite.

* Failure: an indication that something has gone wrong. E.g. the Worker crashed with an OOME, an exception occurred
while doing a map.get or the result of some test didn't give the expected answer. Failures are picked up by the Agent
and send back to the coordinator.

* Worker: a JVM responsible for running a TestSuite.

* Agent: a JVM installed on a piece of hardware. Its main responsibility is spawning, monitoring and terminating workers.

* Coordinator: a JVM that can run anywhere, e.g. on your local machine. You configure it with a list of Agent ip
addresses and you send a command like "run this testsuite with 10 worker JVMs for 2 hours".

* Provisioner: responsible for spawning/terminating EC2 instances and to install Agents on remote machines. It can be
used in combination with EC2 (or any other cloud), but it can also be used in a static setup like a local machine or the
test cluster we have in Istanbul office.