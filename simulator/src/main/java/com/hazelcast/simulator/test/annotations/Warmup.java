package com.hazelcast.simulator.test.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface Warmup {

    /**
     * Global indicates that a single member in the cluster is responsible for the warmup. If not global, then
     * all members in the cluster will do the warmup.
     *
     * If you have a lot of data you want to put in the system, then probably you don't want to use global = true
     * because all loads will be generated through a single member in the cluster.
     *
     * @return
     */
    boolean global() default false;
}
