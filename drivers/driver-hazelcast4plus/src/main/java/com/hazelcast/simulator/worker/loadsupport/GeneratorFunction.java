package com.hazelcast.simulator.worker.loadsupport;

import org.apache.commons.lang3.RandomUtils;

import java.io.Serializable;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Function;

@FunctionalInterface
public interface GeneratorFunction<S, T> extends Function<S, T>, Serializable {

    /**
     * Returns a function that always returns its input argument.
     *
     * @param <T> the type of the input and output objects to the function
     * @return a function that always returns its input argument
     */
    static <T> GeneratorFunction<T, T> identity() {
        return t -> t;
    }

    static <K> GeneratorFunction<K, byte[]> randomBytes(int minLength, int maxLength) {
        return key -> {
            int length = RandomUtils.nextInt(minLength, maxLength);
            byte[] value = new byte[length];
            ThreadLocalRandom.current().nextBytes(value);
            return value;
        };
    }
}
