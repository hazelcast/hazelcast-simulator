/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hazelcast.simulator.test.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotates {@link com.hazelcast.simulator.probes.Probe} fields.
 *
 * @deprecated since 0.10.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface InjectProbe {

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
