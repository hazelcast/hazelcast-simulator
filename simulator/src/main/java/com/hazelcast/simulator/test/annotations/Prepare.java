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
 * In the Prepare expensive things like filling a Map can be done.
 *
 * Multiple {@link Prepare} methods are allowed. The {@link Prepare} methods on a subclass are executed before
 * the {@link Prepare} methods on a super class, however there is no ordering within the same class. This is the same
 * semantics as provided by junit. However local {@link Prepare} always happens before global {@link Prepare}.
 *
 * The prepare method(s) is called only once per test-instance.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface Prepare {

    /**
     * Global indicates that a single member in the cluster is responsible for the warmup. If not global, then
     * all members in the cluster will do the warmup. Be careful that not every worker is going to do the exact
     * same warmup.
     *
     * If you have a lot of data you want to put in the system, then probably you don't want to use global = true
     * because all loads will be generated through a single member in the cluster.
     *
     * @return <tt>true</tt> if global teardown method, <tt>false</tt> otherwise
     */
    boolean global() default false;
}
