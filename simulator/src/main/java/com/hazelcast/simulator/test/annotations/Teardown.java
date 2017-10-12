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
 * Can be placed on a method in a Simulator test that takes care of tearing down the test. E.g. delete resources.
 *
 * Multiple {@link Teardown} methods are allowed. The {@link Teardown} methods on a subclass are executed before
 * the {@link Teardown} methods on a super class, however there is no ordering within the same class. However global
 * teardown will be executed before local teardown.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface Teardown {

    /**
     * Global indicates that a single member in the cluster is responsible for the tear down. If not global, then
     * all members in the cluster will do the teardown.
     *
     * @return <tt>true</tt> if global teardown method, <tt>false</tt> otherwise
     */
    boolean global() default false;
}
