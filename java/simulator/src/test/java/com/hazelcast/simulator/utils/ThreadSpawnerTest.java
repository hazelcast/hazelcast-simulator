package com.hazelcast.simulator.utils;

import com.hazelcast.simulator.utils.helper.CallerInterrupter;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.simulator.TestEnvironmentUtils.setupFakeUserDir;
import static com.hazelcast.simulator.TestEnvironmentUtils.teardownFakeUserDir;
import static com.hazelcast.simulator.utils.CommonUtils.sleepSeconds;
import static com.hazelcast.simulator.utils.FileUtils.getUserDir;
import static com.hazelcast.simulator.utils.TestUtils.assertEqualsStringFormat;
import static org.junit.Assert.assertTrue;

public class ThreadSpawnerTest {

    private Runnable sleepInfiniteRunnable  = new Runnable() {
        @Override
        public void run() {
            sleepSeconds(Integer.MAX_VALUE);
        }
    };

    @Before
    public void before() {
        setupFakeUserDir();
        ExceptionReporter.reset();
    }

    @After
    public void after() {
        teardownFakeUserDir();
    }

    @Test(expected = NullPointerException.class)
    public void testThreadSpawnerNullRunnable() {
        ThreadSpawner spawner = new ThreadSpawner("AnyTestCaseId");
        spawner.spawn(null);
    }

    @Test(expected = NullPointerException.class)
    public void testThreadSpawnerNullPrefix() {
        ThreadSpawner spawner = new ThreadSpawner("AnyTestCaseId");
        spawner.spawn(null, new Runnable() {
            @Override
            public void run() {
            }
        });
    }

    @Test
    public void testThreadSpawner() {
        final AtomicInteger counter = new AtomicInteger(0);

        ThreadSpawner spawner = new ThreadSpawner("AnyTestCaseId");
        for (int i = 0; i < 10; i++) {
            spawner.spawn(new Runnable() {
                @Override
                public void run() {
                    counter.incrementAndGet();
                }
            });
        }
        spawner.awaitCompletion();

        assertEqualsStringFormat("Expected counter to be %d, but as %d", 10, counter.get());
    }

    @Test
    public void testThreadSpawnerWithPrefix() {
        final AtomicInteger counter = new AtomicInteger(0);

        ThreadSpawner spawner = new ThreadSpawner("AnyTestCaseId");
        for (int i = 0; i < 5; i++) {
            spawner.spawn("NamePrefix", new Runnable() {
                @Override
                public void run() {
                    counter.incrementAndGet();
                }
            });
        }
        spawner.awaitCompletion();

        assertEqualsStringFormat("Expected counter to be %d, but as %d", 5, counter.get());
    }

    @Test
    public void testInterrupt() {
        ThreadSpawner spawner = new ThreadSpawner("AnyTestCaseId", true);
        spawner.spawn(sleepInfiniteRunnable);
        spawner.interruptAll();
        spawner.awaitCompletion();
    }

    @Test
    public void testThreadSpawnerException_reportException() {
        File exceptionFile = new File(getUserDir(), "1.exception");

        ThreadSpawner spawner = new ThreadSpawner("AnyTestCaseId");
        spawner.spawn(new Runnable() {
            @Override
            public void run() {
                throw new CommandLineExitException("Expected exception");
            }
        });
        spawner.awaitCompletion();

        assertTrue(exceptionFile.exists());
    }

    @Test(expected = RuntimeException.class)
    public void testThreadSpawnerException_throwException() {
        ThreadSpawner spawner = new ThreadSpawner("AnyTestCaseId", true);
        spawner.spawn(new Runnable() {
            @Override
            public void run() {
                throw new UnsupportedOperationException("Expected exception");
            }
        });
        spawner.awaitCompletion();
    }

    @Test(expected = RuntimeException.class)
    public void testThreadSpawnerInterrupted() {
        new CallerInterrupter(Thread.currentThread(), TimeUnit.SECONDS.toNanos(1)).start();

        ThreadSpawner spawner = new ThreadSpawner("AnyTestCaseId");
        spawner.spawn(sleepInfiniteRunnable);
        spawner.awaitCompletion();
    }
}
