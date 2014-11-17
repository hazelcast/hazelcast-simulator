Writing a test
===========================

A Stabilizer test is a bit like a JUnit test but there are some fundamental differences. Below you can see an example
test where some counter is being incremented.

```
package yourgroupid;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IAtomicLong;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.stabilizer.tests.TestContext;
import com.hazelcast.stabilizer.tests.TestRunner;
import com.hazelcast.stabilizer.tests.annotations.Performance;
import com.hazelcast.stabilizer.tests.annotations.Run;
import com.hazelcast.stabilizer.tests.annotations.Setup;
import com.hazelcast.stabilizer.tests.annotations.Teardown;
import com.hazelcast.stabilizer.tests.annotations.Verify;
import com.hazelcast.stabilizer.tests.utils.ThreadSpawner;

import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.assertEquals;

public class ExampleTest {

    private final static ILogger log = Logger.getLogger(ExampleTest.class);

    //properties
    public int threadCount = 1;
    public int logFrequency = 10000;
    public int performanceUpdateFrequency = 10000;


    private IAtomicLong totalCounter;
    private AtomicLong operations = new AtomicLong();
    private IAtomicLong counter;
    private TestContext testContext;

    @Setup
    public void setup(TestContext testContext) throws Exception {
        this.testContext = testContext;
        HazelcastInstance targetInstance = testContext.getTargetInstance();

        totalCounter = targetInstance.getAtomicLong("totalCounter");
        counter = targetInstance.getAtomicLong("counter");
    }

    @Run
    public void run() {
        ThreadSpawner spawner = new ThreadSpawner(testContext.getTestId());
        for (int k = 0; k < threadCount; k++) {
            spawner.spawn(new Worker());
        }
        spawner.awaitCompletion();
    }

    @Verify
    public void verify() {
        long expected = totalCounter.get();
        long actual = counter.get();

        assertEquals(expected, actual);
    }

    @Teardown
    public void teardown() throws Exception {
        counter.destroy();
        totalCounter.destroy();
    }

    @Performance
    public long getOperationCount() {
        return operations.get();
    }

    private class Worker implements Runnable {
        @Override
        public void run() {
            long iteration = 0;
            while (!testContext.isStopped()) {
                counter.incrementAndGet();

                if (iteration % logFrequency == 0) {
                    log.info(Thread.currentThread().getName() + " At iteration: " + iteration);
                }

                if (iteration % performanceUpdateFrequency == 0) {
                    operations.addAndGet(performanceUpdateFrequency);
                }
                iteration++;
            }

            totalCounter.addAndGet(iteration);
        }
    }

    public static void main(String[] args) throws Throwable {
        ExampleTest test = new ExampleTest();
        new TestRunner(test).run();
    }
}

```

At the end you see a main method; this is useful if you want to run the test locally to see if works at all.

At the top of the source file you also see see 'properties'. When you create a test, you specify the test property file:

```
class=yourgroupid.ExampleTest
threadCount=1
logFrequency=10000
performanceUpdateFrequency=10000
```

The 'class' property defines the actual test case and the rest are the properties you want to bind in your test. If a
property is not defined in the property file, the default value of the property is used.

You can also define multiple tests in a single property file:

```
foo.class=yourgroupid.ExampleTest
foo.threadCount=1

bar.class=yourgroupid.ExampleTest
bar.threadCount=1

```
This is useful if you want to run multiple tests sequentially, or tests in parallel using the 'coordinator --parallel'
option.
