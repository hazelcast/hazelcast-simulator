package com.hazelcast.simulator.utils;

import org.junit.After;
import org.junit.Test;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.simulator.utils.ExecutorFactory.createFixedThreadPool;
import static com.hazelcast.simulator.utils.ReflectionUtils.invokePrivateConstructor;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class ExecutorFactoryTest {

    private ExecutorService executorService;

    @After
    public void tearDown() throws Exception {
        if (executorService != null) {
            executorService.shutdown();
            executorService.awaitTermination(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testConstructor() throws Exception {
        invokePrivateConstructor(ExecutorFactory.class);
    }

    @Test
    public void testFixedThreadPool() throws Exception {
        executorService = createFixedThreadPool(3, ExecutorFactoryTest.class);
        assertNotNull(executorService);

        Future<Boolean> future = executorService.submit(new ExecutorCallable());
        assertTrue(future.get());
    }

    private static final class ExecutorCallable implements Callable<Boolean> {

        @Override
        public Boolean call() throws Exception {
            return true;
        }
    }
}