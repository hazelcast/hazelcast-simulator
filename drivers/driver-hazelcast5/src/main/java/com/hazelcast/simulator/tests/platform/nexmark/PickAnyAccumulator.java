package com.hazelcast.simulator.tests.platform.nexmark;

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
        PickAnyAccumulator that;
        return this == o ||
                o != null
                        && this.getClass() == o.getClass()
                        && this.count == (that = (PickAnyAccumulator) o).count
                        && Objects.equals(this.value, that.value);
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
