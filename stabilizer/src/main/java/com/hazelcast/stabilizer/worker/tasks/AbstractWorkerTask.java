package com.hazelcast.stabilizer.worker.tasks;

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.stabilizer.test.TestContext;
import com.hazelcast.stabilizer.test.annotations.Performance;
import com.hazelcast.stabilizer.worker.OperationSelector;

import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Abstract worker class which is returned by {@link com.hazelcast.stabilizer.test.annotations.RunWithWorker} annotated test
 * methods.
 * <p/>
 * Implicitly logs and measures performance. The related properties can be overwritten with the properties of the test.
 * The Operation counter is automatically increased after each doRun() call.
 *
 * @param <O> Type of Enum used by the {@link com.hazelcast.stabilizer.worker.OperationSelector}
 */
public abstract class AbstractWorkerTask<O extends Enum<O>> implements Runnable {

    final static ILogger LOGGER = Logger.getLogger(AbstractWorkerTask.class);

    final OperationSelector<O> selector;
    final Random random;

    // these fields will be injected by the TestContainer
    TestContext testContext;
    AtomicLong operationCount;

    // these fields will be injected by test.properties of the test
    long logFrequency = 10000;
    long performanceUpdateFrequency = 10000;

    // local variables
    long iteration = 0;

    public AbstractWorkerTask() {
        this.selector = createOperationSelector();
        this.random = new Random();
    }

    @Override
    public void run() {
        while (!testContext.isStopped()) {
            doRun(selector.select());

            increaseIteration();
        }
        operationCount.addAndGet(iteration % performanceUpdateFrequency);
    }

    @Performance
    public long getOperationCount() {
        return operationCount.get();
    }

    protected abstract OperationSelector<O> createOperationSelector();

    protected abstract void doRun(O operation);

    protected int nextInt(int upperBond) {
        return random.nextInt(upperBond);
    }

    void increaseIteration() {
        iteration++;
        if (iteration % logFrequency == 0) {
            LOGGER.info(Thread.currentThread().getName() + " At iteration: " + iteration);
        }

        if (iteration % performanceUpdateFrequency == 0) {
            operationCount.addAndGet(performanceUpdateFrequency);
        }
    }
}
