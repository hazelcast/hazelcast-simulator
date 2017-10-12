/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hazelcast.simulator.worker.selector;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static com.hazelcast.simulator.utils.FormatUtils.NEW_LINE;
import static java.lang.String.format;

/**
 * Builder class to create an {@link OperationSelector} instance. Each operation is represented as an enum item and is registered
 * with its probability (0.0 to 1.0) via the {@link #addOperation(Enum, double)} method.
 *
 * The total sum of probabilities has to be exactly 1.0. You can use {@link #addDefaultOperation(Enum)} to add a default operation
 * which automatically consumes the remaining probability. The probability precision is {@value #PROBABILITY_PRECISION}.
 *
 * This builder class is not thread-safe. The created {@link OperationSelector} does not give any thread-safety guarantees. It is
 * strongly recommended to construct a new instance for each thread, at least to prevent contention on the random generator. Just
 * use a single builder and call the {@link #build()} method in each thread constructor.
 *
 * @param <T> enum of operations
 * @deprecated since 0.9 and will be removed in 0.10.
 */
public class OperationSelectorBuilder<T extends Enum<T>> {

    static final int PROBABILITY_PRECISION = 3;
    static final double PROBABILITY_LENGTH = Math.pow(10, PROBABILITY_PRECISION);
    static final double PROBABILITY_INTERVAL = 1.0 / PROBABILITY_LENGTH;

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
        checkProbabilityArgument(probability);
        if (probability == 0.0) {
            return this;
        }
        if (operations.put(operation, probability) != null) {
            throw new IllegalStateException("Operation " + operation + " has been already added to this selector");
        }
        probSum += probability;
        if (probSum - 1.0 > PROBABILITY_INTERVAL) {
            probabilityMismatch();
        }
        return this;
    }

    /**
     * Register an operation to be returned when no operation registered via {@link #addOperation(Enum, double)} has been
     * selected.
     *
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
     * Returns a set of all defined operations.
     *
     * @return {@link Set} of all defined operations.
     */
    public Set<T> getOperations() {
        return operations.keySet();
    }

    /**
     * Constructs an instance of {@link OperationSelector}.
     *
     * @return instance of OperationSelector
     */
    public OperationSelector<T> build() {
        if (Math.abs(probSum - 1.0) > PROBABILITY_INTERVAL) {
            probabilityMismatch();
        }
        if (operationsArray == null) {
            populateOperationsArray();
        }
        return new OperationSelector<T>(operationsArray);
    }

    private void checkProbabilityArgument(double probability) {
        if (probability < 0.0) {
            throw new IllegalArgumentException("Probability has to be between 0.0 and 1.0, but was " + probability);
        }
        double probabilityDecimalPlaces = probability - Math.floor(probability);
        if (probabilityDecimalPlaces > 0.0 && probabilityDecimalPlaces < PROBABILITY_INTERVAL) {
            throw new IllegalArgumentException(format("Maximum probability precision is %f, but was %f",
                    PROBABILITY_INTERVAL, probability));
        }
    }

    private void probabilityMismatch() {
        StringBuilder sb = new StringBuilder(format("Sum of operation probabilities should be exactly 1.0, but is %f", probSum));
        for (Map.Entry<T, Double> entry : operations.entrySet()) {
            sb.append(NEW_LINE).append("Operation: ").append(entry.getKey()).append(", Probability: ").append(entry.getValue());
        }
        throw new IllegalStateException(sb.toString());
    }

    private void populateOperationsArray() {
        int arraySize = (int) PROBABILITY_LENGTH;
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
    }
}
