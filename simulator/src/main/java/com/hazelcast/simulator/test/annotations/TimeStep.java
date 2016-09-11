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
 * <h1>Code generation</h1>
 * The timestep based tests rely on code generation and the motto is that you should not pay for a feature if it isn't used. For
 * example where logging is not configured, no logging code is generated. If there is only a single timestep method, then there
 * is no randomization. This way we can reduce the overhead by the benchmark framework to the bare minimum.
 *
 * <h1>Execution groups</h1>
 * Normally all timestep methods from a test belong to the same execution group; meaning that there is a group of threads will
 * will call each timestep method using some distribution. But in some cases this is unwanted, e.g. a typical producer/consumer
 * test. In such cases one can make use of execution groups:
 * <pre>
 *     @TimeStep(executionGroup="producer")
 *     public void produce(){
 *         ...
 *     }
 *
 *     @TimeStep(executionGroup="consumer")
 *     public void consume(){
 *
 *     }
 * </pre>
 * In this case there are 2 execution groups: producer and consumer and each will get their own threads where the producer
 * timestep threads calls methods from the 'producer' execution group, and the consumer timestep threads, call methods from
 * the 'consumer' execution-group.
 *
 * <h1>Threadcount</h1>
 * A timestep test run multiple timestep threads in parallel. This can be configured using the threadCount property:
 * <code>
 *     class=yourtest
 *     threadCount=1
 * </code>
 * Threadcount defaults to 10.
 *
 * If there are multiple execution groups, each group can be configured independently. Imagine there is some kind of producer
 * consumer test, then each execution group is configured using:
 * <code>
 *     class=yourtest
 *     producerThreadCount=2
 *     consumerThreadCount=4
 * </code>
 *
 * <h1>Iterations</h1>
 * TimeStep based tests have out of the box support for running a given number of iterations. This can be configured using
 * <code>
 *     class=yourtest
 *     iterations=1000000
 *     warmupIterations=10000
 * </code>
 * This will run each timestep thread for 10k iterations during the warmup, and 1M iterations during the regular run.
 *
 * For the warmupIterations to work, the test needs to be run with a warmupDuration (probably --warmup 0). The test will run
 * as long as the duration or till it runs into a timeout.
 *
 * Each exception group can be configured independently. So imagine there is a producer and consumer execution group, then
 * the producers can be configured using:
 * <code>
 *     class=yourtest
 *     producerWarmupIterations=10000
 *     producerIterations=1000000
 * </code>
 * In this example the producer has a configured number of iterations for warmup and running, the consumer has no such limitation.
 *
 * <h1>Stopping a timestep thread</h1>
 * A Timestep thread can also stop itself by throwing the {@link com.hazelcast.simulator.test.StopException}. This doesn't lead
 * to an error, it is just a signal for the test that the thread is ready.
 *
 * <h1>Logging</h1>
 * By default a timestep based thread will not log anything during the run/warmup period. But sometimes some logging is required,
 * e.g. when needing to do some debugging. There are 2 out of the box options for logging:
 * <ol>
 *     <li>frequency based: e.g. every 1000th iteration</li>
 *     <li>time rate based: e.g. every 100ms</li>
 * </ol>
 *
 * <h2>Frequency based logging</h2>
 * With frequency based logging each timestep thread will add a log entry every so many calls. Frequency based logging can be
 * configured using:
 * <code>
 *     class=yourtest
 *     logFrequency=10000
 * </code>
 * Meaning that each timestep thread will log an entry every 10000th calls.
 *
 * Frequency based logging can be configured per execution group. If there is an execution group producer, then the logFrequency
 * can be configured using:
 * <code>
 *     class=yourtest
 *     producerLogFrequency=10000
 * </code>
 *
 * <h2>Time rate based logging</h2>
 * The time rate based logging allows for a log entry per timestep thread to be written at a maximum rate. E.g. if we want to see
 * at most 1 time a second a log entry, we can configure the test:
 * <code>
 *     class=yourtest
 *     logRateMs=1000
 * </code>
 *
 * Time rate based logging can be configured per execution group. If there is an execution group producer, then the logRate can
 * be configured using:
 * <code>
 *     class=yourtest
 *     producerRateMs=1000
 * </code>
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
