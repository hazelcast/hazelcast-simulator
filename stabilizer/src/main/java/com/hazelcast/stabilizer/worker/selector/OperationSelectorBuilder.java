package com.hazelcast.stabilizer.worker.selector;

import java.util.HashMap;
import java.util.Map;

import static java.lang.String.format;

/**
 * Builder class to create an {@link OperationSelector} instance. Each operation is represented as an enum item and is registered
 * with its probability (0.0 to 1.0) via the {@link #addOperation(Enum, double)} method.
 * <p/>
 * The total sum of probabilities has to be exactly 1.0. You can use {@link #addDefaultOperation(Enum)} to add a default operation
 * which automatically consumes the remaining probability. The probability precision is {@value #PROBABILITY_PRECISION}.
 * <p/>
 * This builder class is not thread-safe. The created {@link OperationSelector} does not give any thread-safety guarantees. It is
 * strongly recommended to construct a new instance for each thread, at least to prevent contention on the random generator. Just
 * use a single builder and call the {@link #build()} method in each thread constructor.
 *
 * @param <T> enum of operations
 */
public class OperationSelectorBuilder<T extends Enum<T>> {
    public static final int PROBABILITY_PRECISION = 3;
    public static final double PROBABILITY_INTERVAL = 1.0 / Math.pow(10, PROBABILITY_PRECISION);

    private final Map<T, Double> operations = new HashMap<T, Double>();

    private double probSum;
    private Object[] operationsArray;

    /**
     * Register a new operation for selection.
     *
     * @param operation   operation to be selected
     * @param probability probability of this operation. It must be a non-negative number between 0.0 and 1.0
     * @return this instance to allow method-chaining
     */
    public OperationSelectorBuilder<T> addOperation(T operation, double probability) {
        if (operationsArray != null) {
            throw new IllegalStateException("The build() method was already called, cannot change operations anymore");
        }
        if (probability == 0.0) {
            return this;
        }
        if (probability < 0.0) {
            throw new IllegalArgumentException("Probability has to be between 0.0 and 1.0, but was " + probability);
        }
        double probabilityDecimalPlaces = probability - Math.floor(probability);
        if (probabilityDecimalPlaces > 0.0 && probabilityDecimalPlaces < PROBABILITY_INTERVAL) {
            throw new IllegalArgumentException(
                    format("Maximum probability precision is %f, but was %f", PROBABILITY_INTERVAL, probability));
        }
        if (operations.put(operation, probability) != null) {
            throw new IllegalStateException("Operation " + operation + " has been already added to this selector");
        }
        probSum += probability;
        if (probSum > 1.0) {
            probabilityMismatch();
        }
        return this;
    }

    /**
     * Register an operation to be returned when no operation registered via {@link #addOperation(Enum, double)} has been selected.
     * <p/>
     * All remaining probability will be consumed by this method.
     *
     * @param operation operation to be selected if no other operation is configured
     * @return this instance to allow method-chaining
     */
    public OperationSelectorBuilder<T> addDefaultOperation(T operation) {
        addOperation(operation, 1.0 - probSum);
        return this;
    }

    /**
     * Constructs an instance of {@link OperationSelector}.
     *
     * @return instance of OperationSelector
     */
    public OperationSelector<T> build() {
        if (1.0 - probSum > PROBABILITY_INTERVAL) {
            probabilityMismatch();
        }
        if (operationsArray == null) {
            populateOperationsArray();
        }
        return new OperationSelector<T>(operationsArray);
    }

    private void probabilityMismatch() {
        StringBuilder sb = new StringBuilder(format("Sum of operation probabilities should be exactly 1.0, but is %f", probSum));
        for (Map.Entry<T, Double> entry : operations.entrySet()) {
            sb.append("\nOperation: ").append(entry.getKey()).append(", Probability: ").append(entry.getValue());
        }
        throw new IllegalStateException(sb.toString());
    }

    private void populateOperationsArray() {
        int arraySize = (int) Math.pow(10, PROBABILITY_PRECISION);
        operationsArray = new Object[arraySize];
        int index = 0;
        for (Map.Entry<T, Double> entry : operations.entrySet()) {
            T operation = entry.getKey();
            for (int i = 0; i < Math.round(entry.getValue() * arraySize); i++) {
                if (index < arraySize) {
                    operationsArray[index] = operation;
                    index++;
                }
            }
        }
        if (index < arraySize) {
            throw new IllegalStateException(format("Operations array is not filled completely (%d/%d)", index, arraySize));
        }
    }
}
