package com.hazelcast.stabilizer.worker.tasks;

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.stabilizer.test.TestContext;
import com.hazelcast.stabilizer.test.annotations.Performance;
import com.hazelcast.stabilizer.test.utils.ThreadSpawner;
import com.hazelcast.stabilizer.worker.selector.OperationSelector;
import com.hazelcast.stabilizer.worker.selector.OperationSelectorBuilder;

import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Abstract worker class which is returned by {@link com.hazelcast.stabilizer.test.annotations.RunWithWorker} annotated test
 * methods.
 * <p/>
 * Implicitly logs and measures performance. The related properties can be overwritten with the properties of the test.
 * The Operation counter is automatically increased after each {@link #timeStep(Enum)} call.
 *
 * @param <O> Type of Enum used by the {@link com.hazelcast.stabilizer.worker.selector.OperationSelector}
 */
public abstract class AbstractWorkerTask<O extends Enum<O>> implements Runnable {

    final static ILogger LOGGER = Logger.getLogger(AbstractWorkerTask.class);

    final Random random = new Random();
    final OperationSelector<O> selector;

    // these fields will be injected by the TestContainer
    TestContext testContext;
    AtomicLong operationCount;

    // these fields will be injected by test.properties of the test
    long logFrequency = 10000;
    long performanceUpdateFrequency = 10;

    // local variables
    long iteration = 0;

    public AbstractWorkerTask(OperationSelectorBuilder<O> operationSelectorBuilder) {
        this.selector = operationSelectorBuilder.build();
    }

    /**
     * This constructor is just for child classes who also override the {@link #run()} method.
     */
    AbstractWorkerTask() {
        this.selector = null;
    }

    @Override
    public void run() {
        beforeRun();

        while (!testContext.isStopped()) {
            timeStep(selector.select());

            increaseIteration();
        }
        operationCount.addAndGet(iteration % performanceUpdateFrequency);

        afterRun();
    }

    @Performance
    public long getOperationCount() {
        return operationCount.get();
    }

    protected void setPerformanceUpdateFrequency(long performanceUpdateFrequency) {
        if (performanceUpdateFrequency <= 0) {
            throw new IllegalArgumentException("performanceUpdateFrequency must be a positive number!");
        }
        this.performanceUpdateFrequency = performanceUpdateFrequency;
    }

    /**
     * Override this method if you need to execute code on each worker before {@link #run()} is called.
     */
    protected void beforeRun() {
    }

    /**
     * This method is called for each iteration of {@link #run()}.
     * <p/>
     * Won't be called if an error occurs in {@link #beforeRun()}.
     *
     * @param operation The selected operation for this iteration
     */
    protected abstract void timeStep(O operation);

    /**
     * Override this method if you need to execute code on each worker after {@link #run()} is called.
     * <p/>
     * Won't be called if an error occurs in {@link #beforeRun()} or {@link #timeStep(Enum)}.
     */
    protected void afterRun() {
    }

    /**
     * Override this method if you need to execute code once after all workers have finished their run phase.
     * <p/>
     * Will be executed after {@link ThreadSpawner#awaitCompletion()} on a single worker instance.
     */
    public void afterCompletion() {
    }

    /**
     * Calls {@link Random#nextInt()} on an internal Random instance.
     *
     * @return the next pseudo random, uniformly distributed {@code int} value from this random number generator's sequence
     */
    protected int randomInt() {
        return random.nextInt();
    }

    /**
     * Calls {@link Random#nextInt(int)} on an internal Random instance.
     *
     * @return the next pseudo random, uniformly distributed {@code int} value between {@code 0} (inclusive) and {@code n}
     * (exclusive) from this random number generator's sequence
     */
    protected int randomInt(int upperBond) {
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
