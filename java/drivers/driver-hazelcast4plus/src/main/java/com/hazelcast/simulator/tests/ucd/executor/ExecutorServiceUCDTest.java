package com.hazelcast.simulator.tests.ucd.executor;

import com.hazelcast.core.IExecutorService;
import com.hazelcast.simulator.probes.LatencyProbe;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Teardown;
import com.hazelcast.simulator.test.annotations.TimeStep;
import com.hazelcast.simulator.tests.ucd.UCDTest;

import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class ExecutorServiceUCDTest extends UCDTest {
    private IExecutorService executor;

    @Override
    @Setup
    public void setUp() throws ReflectiveOperationException  {
        executor = targetInstance.getExecutorService(name);
        super.setUp();
        Callable<Long> udf = getUDFInstance(System.nanoTime());
        executor.submit(udf);
    }

    @TimeStep
    public void timeStep(LatencyProbe latencyProbe) throws Exception {
        Callable<Long> udf = getUDFInstance(System.nanoTime());
        Future<Long> future = executor.submit(udf);

        long start = future.get();
        latencyProbe.done(start);
    }

    @Teardown(global = true)
    public void teardown() throws InterruptedException {
        executor.shutdownNow();
        if (!executor.awaitTermination(120, TimeUnit.SECONDS)) {
            logger.fatal("Time out while waiting for shutdown of executor: {}", executor.getName());
        }
        executor.destroy();
    }
}
