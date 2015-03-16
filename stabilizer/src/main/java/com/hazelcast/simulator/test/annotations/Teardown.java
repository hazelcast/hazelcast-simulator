package com.hazelcast.simulator.test.annotations;


import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface Teardown {

    /**
     * Global indicates that a single member in the cluster is responsible for the tear down. If not global, then
     * all members in the cluster will do the teardown.
     *
     * @return
     */
    boolean global() default false;
}
