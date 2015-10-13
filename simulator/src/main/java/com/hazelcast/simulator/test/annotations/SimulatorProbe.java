package com.hazelcast.simulator.test.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotates {@link com.hazelcast.simulator.probes.Probe} fields.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface SimulatorProbe {

    String NULL = "probe name default";

    /**
     * Defines the probe name.
     *
     * @return the probe name
     */
    String name() default NULL;

    /**
     * Defines if a probe should be used for the calculation of test throughput.
     *
     * @return <tt>true</tt> if probe should be considered for throughput, <tt>false</tt> otherwise
     */
    boolean useForThroughput() default false;
}
