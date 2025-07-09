package com.hazelcast.simulator.drivers;

/**
 * Used for converting driver instances into other types when binding
 * the instance to fields via the {@link com.hazelcast.simulator.test.annotations.InjectDriver}
 * annotation.
 */
public interface Convertible {
    /**
     * Converts this object into the target type. Note any class which
     * implements this interface <b>must</b> support the identity conversion,
     * i.e. converting to its own class by returning itself (or a copy).
     *
     * @param target The type to convert the instance to
     * @return The converted instance if the type is supported, null otherwise
     */
    Object convertTo(Class<?> target);
}
