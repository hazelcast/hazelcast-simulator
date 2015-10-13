package com.hazelcast.simulator.tests.special;

import com.hazelcast.simulator.test.TestContext;
import com.hazelcast.simulator.test.annotations.Run;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Teardown;
import com.hazelcast.simulator.test.annotations.Verify;
import com.hazelcast.simulator.test.annotations.Warmup;

import java.util.concurrent.TimeUnit;

import static com.hazelcast.simulator.utils.CommonUtils.sleepSeconds;

/**
 * Used to test timeout detection of {@link com.hazelcast.simulator.agent.workerjvm.WorkerJvmFailureMonitor}.
 */
public class LongTestPhasesTest {

    private static final int TWO_MINUTES = (int) TimeUnit.MINUTES.toSeconds(2);

    @Setup
    public void setUp(TestContext testContext) {
        sleepSeconds(TWO_MINUTES);
    }

    @Teardown
    public void tearDown() {
        sleepSeconds(TWO_MINUTES);
    }

    @Warmup(global = false)
    public void warmup() {
        sleepSeconds(TWO_MINUTES);
    }

    @Verify(global = false)
    public void verify() {
        sleepSeconds(TWO_MINUTES);
    }

    @Run
    public void run() {
        sleepSeconds(TWO_MINUTES);
    }
}
