package com.hazelcast.simulator.tests.special;

import com.hazelcast.simulator.test.TestContext;
import com.hazelcast.simulator.test.annotations.Run;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Teardown;
import com.hazelcast.simulator.test.annotations.Verify;
import com.hazelcast.simulator.test.annotations.Warmup;

import static com.hazelcast.simulator.utils.CommonUtils.sleepSeconds;

/**
 * Used to test timeout detection of {@link com.hazelcast.simulator.agent.workerjvm.WorkerJvmFailureMonitor}.
 */
public class LongTestPhasesTest {

    // properties
    public boolean allPhases = false;
    public int sleepSeconds = 120;

    @Setup
    public void setUp(TestContext testContext) {
        sleepConditional();
    }

    @Teardown
    public void tearDown() {
        sleepConditional();
    }

    @Warmup(global = false)
    public void warmup() {
        sleepConditional();
    }

    @Verify(global = false)
    public void verify() {
        sleepConditional();
    }

    @Run
    public void run() {
        sleepSeconds(sleepSeconds);
    }

    private void sleepConditional() {
        if (allPhases) {
            sleepSeconds(sleepSeconds);
        }
    }
}
