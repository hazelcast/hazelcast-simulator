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
 * Needs to be placed on a load generating method of a test.
 *
 * A method should contain either:
 * <ol>
 *     <li>{@link Run}</li>
 *     <li>{@link RunWithWorker}</li>
 *     <li>{@link TimeStep}</li>
 * </ol>
 * The {@link TimeStep} is the one that should be picked by default. It is the most powerful and it relies on code generation
 * to create a runner with the least amount of overhead.  The {@link TimeStep} looks a lot like the  @Benchmark from JMH.
 *
 * <h1>Iterations</h1>
 * TimeStep based tests have out of the box support for running a given number of iterations. This can be configured using
 * <code>
 *     class=yourtest
 *     iterations=1000000
 *     warmupIterations=10000
 * </code>
 * For the warmupIterations to work, the test needs to be run with a warmupDuration (probably --warmup 0). The test will run
 * as long as the duration or till it runs into a timeout.
 *
 * <h1>Stopping a timestep thread</h1>
 * A Timestep thread can also stop itself by throwing the {@link com.hazelcast.simulator.test.StopException}. This doesn't lead
 * to an error, it is just a signal for the test that the thread is ready.
 *
 * @see BeforeRun
 * @see AfterRun
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface TimeStep {

    /**
     * The probability of this method running. 0 means there is 0% chance, and 1 means there is 100% chance.
     *
     * The sum of probability of all timestep methods needs to be one.
     *
     * Compatibility mode:
     * If the probability is -1, it will receive all remaining probability. E.g. if there is a put configured with probability
     * 0.2, and get with -1 and put and get are the only 2 options, then the eventual probability for the get will be 1-0.2=0.8.
     *
     * The reason this is done is to provide compatibility with the old way of probability configuration where one operation
     * received whatever remains.
     *
     * @return the probability.
     */
    double prob() default 1;

    /**
     * Normally all timeStep methods will be executed by a single executionGroup of threads. But in some case you need to have
     * some methods executed by one executionGroup of threads, and other methods by other groups threads. A good example would
     * be a produce/consume example where the produce timeStep methods are called by different methods than consume timestep
     * methods.
     *
     * Normally threadCount is configured using 'threadCount=5'. In case of 'foobar' executionGroup, the threadCount is
     * configured using 'foobarThreadCount=5'.
     *
     * This setting is copied from JMH, see:
     * http://javadox.com/org.openjdk.jmh/jmh-core/0.9/org/openjdk/jmh/annotations/Group.html
     *
     * @return the executionGroup.
     */
    String executionGroup() default "";
}
