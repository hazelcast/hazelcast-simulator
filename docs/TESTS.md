Writing a test
===========================

A Simulator test is a bit like a JUnit test but there are some fundamental differences. Below you can see an example
test where some counter is being incremented.

```
package yourGroupId;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.simulator.test.TestContext;
import com.hazelcast.simulator.test.TestRunner;
import com.hazelcast.simulator.test.annotations.*;
import com.hazelcast.simulator.worker.selector.OperationSelectorBuilder;
import com.hazelcast.simulator.worker.tasks.AbstractWorker;

import static junit.framework.TestCase.assertEquals;

public class ExampleTest {

    private enum Operation {
        PUT,
        GET
    }

    private static final ILogger log = Logger.getLogger(ExampleTest.class);

    //properties
    public double putProb = 0.5;
    public int maxKeys = 1000;

    private TestContext testContext;
    private IMap map;

    private OperationSelectorBuilder<Operation> operationSelectorBuilder = new OperationSelectorBuilder<Operation>();

    @Setup
    public void setup(TestContext testContext) throws Exception {
        log.info("======== SETUP =========");
        this.testContext = testContext;
        HazelcastInstance targetInstance = testContext.getTargetInstance();
        map = targetInstance.getMap("exampleMap");

        log.info("Map name is:" + map.getName());

        operationSelectorBuilder.addOperation(Operation.PUT, putProb).addDefaultOperation(Operation.GET);
    }

    @Warmup
    public void warmup() {
        log.info("======== WARMUP =========");
        log.info("Map size is:" + map.size());
    }

    @Verify
    public void verify() {
        log.info("======== VERIFYING =========");
        log.info("Map size is:" + map.size());

        for (int i = 0; i < maxKeys; i++) {
            assertEquals(map.get(i), "value" + i);
        }
    }

    @Teardown
    public void teardown() throws Exception {
        log.info("======== TEAR DOWN =========");
        map.destroy();
        log.info("======== THE END =========");
    }

    @RunWithWorker
    public AbstractWorker<Operation> createWorker() {
        return new Worker();
    }

    private class Worker extends AbstractWorker<Operation> {

        public Worker() {
            super(operationSelectorBuilder);
        }

        @Override
        protected void timeStep(Operation operation) {
            int key = randomInt(maxKeys);
            switch (operation) {
                case PUT:
                    map.put(key, "value" + key);
                    break;
                case GET:
                    map.get(key);
                    break;
                default:
                    throw new UnsupportedOperationException("Unknown operation" + operation);
            }
        }

    }

    public static void main(String[] args) throws Throwable {
        ExampleTest test = new ExampleTest();
        new TestRunner<ExampleTest>(test).run();
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
