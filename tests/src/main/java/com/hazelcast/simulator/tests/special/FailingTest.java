package com.hazelcast.simulator.tests.special;

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.simulator.test.TestContext;
import com.hazelcast.simulator.test.TestException;
import com.hazelcast.simulator.test.TestRunner;
import com.hazelcast.simulator.test.annotations.Run;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.utils.EmptyStatement;
import com.hazelcast.simulator.utils.ExceptionReporter;

import java.util.LinkedList;
import java.util.List;

import static com.hazelcast.simulator.utils.CommonUtils.exitWithError;

/**
 * A test that causes a failure. This is useful for testing the simulator framework and for demonstration purposes.
 */
public class FailingTest {

    public enum Failure {
        EXCEPTION,
        OOME,
        EXIT
    }

    private static final ILogger LOGGER = Logger.getLogger(FailingTest.class);

    // properties
    public Failure failure = Failure.EXCEPTION;

    private TestContext testContext;

    @Setup
    public void setUp(TestContext testContext) {
        this.testContext = testContext;
    }

    @Run
    public void run() {
        switch (failure) {
            case EXCEPTION:
                ExceptionReporter.report(testContext.getTestId(), new TestException("Wanted exception"));
                break;
            case OOME:
                List<byte[]> list = new LinkedList<byte[]>();
                for (; ; ) {
                    try {
                        list.add(new byte[100 * 1000 * 1000]);
                    } catch (OutOfMemoryError ignored) {
                        EmptyStatement.ignore(ignored);
                        break;
                    }
                }
                LOGGER.severe("We should never reach this code! List size: " + list.size());
                break;
            case EXIT:
                exitWithError();
                break;
            default:
                throw new UnsupportedOperationException("Unknown failure " + failure);
        }
    }

    public static void main(String[] args) throws Exception {
        FailingTest test = new FailingTest();
        new TestRunner<FailingTest>(test).run();
    }
}
