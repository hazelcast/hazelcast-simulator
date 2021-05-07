/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.simulator.tests.platform.nexmark.accumulator;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.StreamSerializer;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Objects;

public class PickAnyAccumulator<T> {

    private T value;
    private long count;

    public PickAnyAccumulator() {
    }

    public PickAnyAccumulator(T value, long count) {
        this.value = value;
        this.count = count;
    }

    public void accumulate(@Nullable T t) {
        if (t == null) {
            return;
        }
        value = t;
        count++;
    }

    public void combine(@Nonnull PickAnyAccumulator<T> other) {
        count += other.count;
        if (value == null) {
            value = other.value;
        }
    }

    public void deduct(@Nonnull PickAnyAccumulator<T> other) {
        count -= other.count;
        assert count >= 0 : "Negative count after deduct";
        if (count == 0) {
            value = null;
        }
    }

    public T get() {
        return value;
    }

    @Override
    @SuppressWarnings("rawtypes")
    public boolean equals(Object o) {
        return this == o
                || o != null
                        && this.getClass() == o.getClass()
                        && this.count == ((PickAnyAccumulator) o).count
                        && Objects.equals(this.value, ((PickAnyAccumulator) o).value);
    }

    @Override
    public int hashCode() {
        long hc = 17;
        hc = 73 * hc + count;
        hc = 73 * hc + (value != null ? value.hashCode() : 0);
        return Long.hashCode(hc);
    }

    @Override
    public String toString() {
        return "MutableReference(" + value + ')';
    }

    public static final class PickAnyAccumulatorSerializer<T> implements StreamSerializer<PickAnyAccumulator<T>> {

        @Override
        public int getTypeId() {
            return 10;
        }

        @Override
        public void write(ObjectDataOutput out, PickAnyAccumulator<T> acc) throws IOException {
            out.writeObject(acc.value);
            out.writeLong(acc.count);
        }

        @Override
        public PickAnyAccumulator<T> read(ObjectDataInput in) throws IOException {
            return new PickAnyAccumulator<>(in.readObject(), in.readLong());
        }
    }
}
