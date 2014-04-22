package com.hazelcast.stabilizer.worker.testcommands;

public interface TestCommandMBean {
    Object execute(TestCommand testCommand) throws Exception;
}
