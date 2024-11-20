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
 * Is placed on a method in a test so for verification. E.g. when an
 * IAtomicLong.inc is tested, in the verify method one could check if the counter
 * of the IAtomicLong matches the actual number of increments.
 * <p/>
 * Multiple {@link Verify} methods are allowed. The {@link Verify} methods on a
 * subclass are executed before the {@link Verify} methods on a super class,
 * however there is no ordering within the same class. This is the same semantics
 * as provided by junit. However {@link Verify} local happens before global
 * {@link Verify}.
 * <p/>
 * The verify method(s) is called at most once per test-instance.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface Verify {

    /**
     * Global indicates that a single worker in the cluster is responsible for
     * the verify. If not global, then all workers in the cluster will do the verify.
     *
     * @return <tt>true</tt> if global verify method, <tt>false</tt> otherwise
     */
    boolean global() default true;
}
