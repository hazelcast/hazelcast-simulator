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

import java.util.Random;

/**
 * Facility to select different operations based on probabilities. Each operations is represented as an enum item.
 *
 * Calling {@link #select()} method will select an operation according to the configured probabilities.
 *
 * This class does not give any thread-safety guarantees. It is strongly recommended to construct a new instance for each thread,
 * at least to prevent contention on the random generator. Just use a single builder and call the
 * {@link OperationSelectorBuilder#build()} method in each thread constructor.
 *
 * @param <T> enum of operations
 *
 * @deprecated since 0.9 and will be removed in 0.10.
 */
public class OperationSelector<T extends Enum<T>> {

    private final Random random = new Random();
    private final Object[] operations;
    private final int length;

    OperationSelector(Object[] operations) {
        this.operations = operations.clone();
        this.length = operations.length;
    }

    /**
     * Select an operation according to configured probabilities.
     *
     * @return selected operation
     */
    @SuppressWarnings("unchecked")
    public T select() {
        int chance = random.nextInt(length);
        return (T) operations[chance];
    }
}
