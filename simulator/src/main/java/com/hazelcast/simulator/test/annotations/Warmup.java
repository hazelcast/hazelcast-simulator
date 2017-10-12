/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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
 * @deprecated since Simulator 0.9. Use {@link Prepare} instead. This reason this class is deprecated, is that the name warmup
 * is confusing since it is being used to heat up the jit, but also it is used to fill the data-structures.
 */
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
     * @return <tt>true</tt> if global teardown method, <tt>false</tt> otherwise
     */
    boolean global() default false;
}
