package com.hazelcast.stabilizer.worker;

import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Facility to select different operations based on probabilities. Each operations is
 * represented as an enum item and is registered with its probability (0-1) via the {@link #addOperation(Enum, double)} method.
 *
 * Calling {@link #select()} method will select an operation according configured probabilities. It can return null
 * if sum of configured probabilities if lower than 1. You can change this behaviour by {@link #empty(Enum)} method.
 *
 * @param <T>
 */
public class OperationSelector<T extends Enum<T>> {
    private final Random r;
    private final Map<T, Double> ops;
    private T emptyOperation;
    private double remaining;

    public OperationSelector() {
        r = new Random();
        ops = new ConcurrentHashMap<T, Double>();
        remaining = 1;
    }

    /**
     * Register new operation for selection.
     *
     * @param operation operation to be selected
     * @param probability probability of this operation. It must be a non-negative number between 0 - 1.
     * @return this instance to allow method-chaining
     */
    public OperationSelector<T> addOperation(T operation, double probability) {
        if (ops.put(operation, probability) != null) {
            throw new IllegalStateException("Operations " + operation + " has been already added to this selector.");
        }
        remaining -= probability;
        if (remaining < -0.1) {
            return onProbabilityExceeded();
        }
        return this;
    }

    /**
     * Register an operation to be returned when no operation registered via {@link #addOperation(Enum, double)} has been
     * selected.
     *
     * @param operation
     * @return
     */
    public OperationSelector<T> empty(T operation) {
        this.emptyOperation = operation;
        return this;
    }

    /**
     * Select an operation according to probabilities
     *
     * @return selected operation or null if no operation has been selected
     */
    public T select() {
        double chance = r.nextDouble();
        for (Map.Entry<T, Double> entry : ops.entrySet()) {
            Double probability = entry.getValue();
            if ((chance -= probability) < 0) {
                return entry.getKey();
            }
        }
        return emptyOperation;
    }

    private OperationSelector<T> onProbabilityExceeded() {
        StringBuilder builder = new StringBuilder("Sum of probabilities of all operations can't be greater than 0. " +
                "Current operations and probabalities: ").append('\n');
        double total = 0;
        for (Map.Entry<T, Double> entry : ops.entrySet()) {
            Double probability = entry.getValue();
            builder.append("Operation: ").append(entry.getKey()).append(", Probability: ").append(probability);
            total += probability;
        }
        builder.append("\nThis means current sum of probabilities is ").append(total);
        throw new IllegalStateException(builder.toString());
    }

}
