package com.hazelcast.stabilizer.common.messaging;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Subclass of {@link com.hazelcast.stabilizer.common.messaging.Message} annotated with
 * {@link com.hazelcast.stabilizer.common.messaging.MessageSpec} are auto-registered by
 * {@link com.hazelcast.stabilizer.common.messaging.MessagesFactory} and can be constructed
 * by calling {@link com.hazelcast.stabilizer.common.messaging.Message#newBySpec(String, String)}
 *
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface MessageSpec {

    /**
     * Specification of the message to be used when calling {@link com.hazelcast.stabilizer.common.messaging.Message#newBySpec(String, String)}
     */
    public String value();
}
