package com.hazelcast.simulator.tests;

import com.hazelcast.simulator.test.TestContext;
import com.hazelcast.simulator.test.annotations.AfterRun;
import com.hazelcast.simulator.test.annotations.Prepare;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Teardown;
import com.hazelcast.simulator.test.annotations.TimeStep;
import com.hazelcast.simulator.test.annotations.Verify;

import static com.hazelcast.simulator.utils.CommonUtils.sleepSeconds;

public class SuccessTimeStepTest {
    private TestContext context;

    @Setup
    public void setUp(TestContext context) {
        this.context = context;
    }

    @Teardown(global = false)
    public void localTearDown() {
    }

    @Teardown(global = true)
    public void globalTearDown() {
    }

    @Prepare(global = false)
    public void localPrepare() {
        sleepSeconds(1);
    }

    @Prepare(global = true)
    public void globalPrepare() {
        sleepSeconds(1);
    }

    @Verify(global = false)
    public void localVerify() {
    }

    @Verify(global = true)
    public void globalVerify() {
    }

    @TimeStep
    public void run() {
        sleepSeconds(1);
        System.out.println("testSuccess running");
    }

    @AfterRun
    public void afterRun() {
        System.out.println("testSuccess completed");
    }
}
