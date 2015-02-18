package com.hazelcast.stabilizer.worker.selector;

import java.util.Random;

/**
 * Facility to select different operations based on probabilities. Each operations is represented as an enum item.
 * <p/>
 * Calling {@link #select()} method will select an operation according to the configured probabilities.
 * <p/>
 * This class does not give any thread-safety guarantees. It is strongly recommended to construct a new instance for each thread,
 * at least to prevent contention on the random generator. Just use a single builder and call the
 * {@link OperationSelectorBuilder#build()} method in each thread constructor.
 *
 * @param <T> enum of operations
 */
public class OperationSelector<T extends Enum<T>> {
    private final Random random = new Random();
    private final Object[] operations;

    OperationSelector(Object[] operations) {
        this.operations = operations;
    }

    /**
     * Select an operation according to configured probabilities.
     *
     * @return selected operation
     */
    @SuppressWarnings("unchecked")
    public T select() {
        int chance = random.nextInt(operations.length);
        return (T) operations[chance];
    }
}
