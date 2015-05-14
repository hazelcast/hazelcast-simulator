package com.hazelcast.simulator.common.messaging;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Subclass of {@link Message} annotated with {@link MessageSpec} are auto-registered by {@link MessagesFactory} and can be
 * constructed by calling {@link Message#newBySpec(String, String)}
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface MessageSpec {

    /**
     * Specification of the message to be used when calling {@link Message#newBySpec(String, String)}
     *
     * @return message specification
     */
    String value();

    /**
     * A human-readable description of a message. It can be used to generate <tt>--help</tt>
     *
     * @return human-readable description
     */
    String description() default "No description provided";
}
