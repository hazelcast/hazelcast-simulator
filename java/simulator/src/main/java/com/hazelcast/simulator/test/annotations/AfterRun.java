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
 * Is run after a thread is executing its {@link TimeStep} methods.
 * <p/>
 * For every load generating thread, there will be one call to the {@link AfterRun}
 * method. If the test has no {@link TimeStep} methods, methods with {@link AfterRun}
 * are ignored.
 * <p/>
 * An AfterRun is only called if preceding methods like BeforeRun, TimeStep and
 * other AfterRun methods have completed without exception.
 * <p/>
 * Multiple {@link AfterRun} methods are allowed. The {@link AfterRun} method on
 * a subclass is executed before the {@link AfterRun} methods on a super class,
 * however there is no ordering within the same class. This is the same semantics
 * as provided by junit.
 *
 * @see TimeStep
 * @see BeforeRun
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface AfterRun {

    /**
     * The execution executionGroup. For more information see {@link TimeStep}.
     *
     * @return the execution executionGroup.
     */
    String executionGroup() default "";
}
