package com.hazelcast.simulator.worker;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.simulator.test.TestCase;
import com.hazelcast.simulator.test.TestContainer;
import com.hazelcast.simulator.test.TestContext;
import com.hazelcast.simulator.test.TestContextImpl;
import com.hazelcast.simulator.test.TestPhase;
import com.hazelcast.simulator.test.annotations.AfterRun;
import com.hazelcast.simulator.test.annotations.BeforeRun;
import com.hazelcast.simulator.test.annotations.TimeStep;
import org.junit.Test;

import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

import static com.hazelcast.simulator.TestSupport.spawn;

/**
 * Created by alarmnummer on 7/9/16.
 */
public class TimeStepIntegrationTest {

    @Test
    public void test() throws Exception {
        TestCase testCase = new TestCase("id")
                .setProperty("class", Foo.class.getName());

        Foo foo = new Foo();
        TestContextImpl testContext = new TestContextImpl(testCase.getId());
        final TestContainer container = new TestContainer(testContext, foo, testCase);

        Future setupFuture = spawn(new Callable(){
            @Override
            public Object call() throws Exception {
                container.invoke(TestPhase.SETUP);
                return null;
            }
        });
        setupFuture.get();

        container.invoke(TestPhase.RUN);

        container.invoke(TestPhase.LOCAL_TEARDOWN);

        Thread.sleep(5000);
        testContext.stop();

        System.out.println("done");
    }



    public static class Foo {
        private final AtomicLong beforeRunCount = new AtomicLong();
        private final AtomicLong afterRunCount = new AtomicLong();
        private final AtomicLong timeStepCount = new AtomicLong();


        @BeforeRun
        public void beforeRun() {
            beforeRunCount.incrementAndGet();
        }

        @TimeStep
        public void timestep() {
            timeStepCount.incrementAndGet();
        }

        @AfterRun
        public void afterRun() {
            afterRunCount.incrementAndGet();
        }
    }
}
