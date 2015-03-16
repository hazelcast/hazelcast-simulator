package com.hazelcast.simulator.test.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface Verify {

    /**
     * Global indicates that a single member in the cluster is responsible for the verify. If not global, then
     * all members in the cluster will do the verify.
     *
     * @return
     */
    boolean global() default true;
}
