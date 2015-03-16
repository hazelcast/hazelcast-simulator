package com.hazelcast.simulator.test.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * This annotation is here temporary to let a test expose a method to indicate performance
 * In the future we'll get a much better system that probably will be accessible through
 * the TestContext to setup all kinds of performance metrics.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface Performance {
}
