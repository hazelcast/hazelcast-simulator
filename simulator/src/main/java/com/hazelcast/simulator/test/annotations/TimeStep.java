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
 * <li>{@link Run}</li>
 * <li>{@link RunWithWorker}</li>
 * <li>{@link TimeStep}</li>
 * </ol>
 * The {@link TimeStep} is the one that should be picked by default. It is the most powerful and it relies on code generation
 * to create a runner with the least amount of overhead.  The {@link TimeStep} looks a lot like the  @Benchmark from JMH.
 *
 * <h1>Probabilities</h1>
 * THe simplest timestep test has a single timestep method, but in reality there is often some kind of ratio, e.g. 10% write
 * and 90% read. This can be done configuring probability:
 * <pre>
 * {@code
 *     &#064;TimeStep(prob=0.9)
 *     public void read(){
 *         ...
 *     }
 *
 *     &#064;TimeStep(prob=0.10)
 *     public void write(){
 *
 *     }
 * }
 * </pre>
 * The sum of the probabilities needs to be 1.
 *
 * For backwards compatibility reasons one of the timestep methods can be the 'default' by assigning -1 to its probability.
 * It means that whatever remains, will be given to that method. For example the above 90/10 could also be configured using:
 * <pre>
 * {@code
 *     &#064;TimeStep(prob=0.9)
 *     public void read(){
 *         ...
 *     }
 *
 *     &#064;TimeStep(prob=-1)
 *     public void write(){
 *
 *     }
 * }
 * </pre>
 *
 * <h1>Thread state</h1>
 * In a lot of cases a timestep thread needs to have some thread specific context e.g. counters. THe thread-state needs to be
 * a public class with a public no arg constructor, or a constructor receiving the test instance.
 *
 * <pre>
 * {@code
 *     &#064;TimeStep
 *     public void timestep(ThreadState context){
 *        ...
 *     }
 *
 *     public class ThreadContext{
 *         int counter;
 *     }
 * }
 * </pre>
 * Each thread will get its own instance of the ThreadContext. In practice you probably want to extend from the
 * {@link com.hazelcast.simulator.test.BaseThreadState} since it has convenience functions for randomization. The Thread state
 * can be used on the TimeStep methods, but also in the {@link BeforeRun} and {@link AfterRun} methods where the {@link BeforeRun}
 * can take of initialization, and the {@link AfterRun} can take care of some post processing. For a full example see the
 * AtomicLongTest.
 *
 * <h1>Code generation</h1>
 * The timestep based tests rely on code generation for the actual code to call the timestep methods. This prevents the need
 * for reflection and and the motto is that you should not pay for a feature if it isn't used. For example where logging is not
 * configured, no logging code is generated. If there is only a single timestep method, then there is no randomization. This way
 * we can reduce the overhead by the benchmark framework to the bare minimum. The generated code of the TimeStepRunner can be
 * found in the worker directory.
 *
 * <h1>Execution groups</h1>
 * Normally all timestep methods from a test belong to the same execution group; meaning that there is a group of threads will
 * will call each timestep method using some distribution. But in some cases this is unwanted, e.g. a typical producer/consumer
 * test. In such cases one can make use of execution groups:
 * <pre>
 * {@code
 *     &#064;TimeStep(executionGroup="producer")
 *     public void produce(){
 *         ...
 *     }
 *
 *     &#064;TimeStep(executionGroup="consumer")
 *     public void consume(){
 *          ...
 *     }
 * }
 * </pre>
 * In this case there are 2 execution groups: producer and consumer and each will get their own threads where the producer
 * timestep threads calls methods from the 'producer' execution group, and the consumer timestep threads, call methods from
 * the 'consumer' execution-group.
 *
 * Most of the features are configured per timestep group and by default the group "" is used. So you can configure probabilities,
 * metronomes, thread context etc per execution group.
 *
 * <h1>Threadcount</h1>
 * A timestep test run multiple timestep threads in parallel. This can be configured using the threadCount property:
 * <pre>
 * {@code
 *     class=yourtest
 *     threadCount=1
 * }
 * </pre>
 * Threadcount defaults to 10.
 *
 * If there are multiple execution groups, each group can be configured independently. Imagine there is some kind of producer
 * consumer test, then each execution group is configured using:
 * <pre>
 * {@code
 *     class=yourtest
 *     producerThreadCount=2
 *     consumerThreadCount=4
 * }
 * </pre>
 *
 * <h1>Iterations</h1>
 * TimeStep based tests have out of the box support for running a given number of iterations. This can be configured using
 * <pre>
 * {@code
 *     class=yourtest
 *     iterations=1000000
 * }
 * </pre>
 * This will run 1M iterations per worker-thread.
 *
 * Each exception group can be configured independently. So imagine there is a producer and consumer execution group, then
 * the producers can be configured using:
 * <pre>
 * {@code
 *     class=yourtest
 *     producerIterations=1000000
 * }
 * </pre>
 * In this example the producer has a configured number of iterations for running, the consumer has no such limitation.
 *
 * <h1>Stopping a timestep thread</h1>
 * A Timestep thread can also stop itself by throwing the {@link com.hazelcast.simulator.test.StopException}. This doesn't lead
 * to an error, it is just a signal for the test that the thread is ready.
 *
 * <h1>Probes</h1>
 * By default every timestep method will get its own probe. E.g.
 * <pre>
 * {@code
 *     &#064;TimeStep(prob=0.9)
 *     public void read(){
 *         ...
 *     }
 *
 *     &#064;TimeStep(prob=0.10)
 *     public void write(){
 *          ...
 *     }
 * }
 * </pre>
 * In this case 2 probes that keep track of writer or read. So by default tracking latency is taken care of by the timestep
 * runner. In some cases, especially with async testing, the completion of the system being tested, doesn't align with the
 * completion of the timestep method. So the latency can't be determined by the timestep runner. In such cases one can
 * get access to the Probe and startTime like this:
 * <pre>
 * {@code
 *     &#064;TimeStep(prob=0.9)
 *     public void asyncCall(Probe probe, @StartNanos long startNanos){
 *         ...
 *     }
 * }
 * </pre>
 * One can decide to e.g. make use of an completion listener to determine when the call completed and record the right latency
 * on the probe.
 *
 * Keep in mind that the current iteration (and therefor numbers like throughput) are based on completion of the timestep method,
 * but that doesn't need to mean completion of the async call.
 *
 * <h1>Logging</h1>
 * By default a timestep based thread will not log anything during the run period. But sometimes some logging is required,
 * e.g. when needing to do some debugging. There are 2 out of the box options for logging:
 * <ol>
 * <li>frequency based: e.g. every 1000th iteration</li>
 * <li>time rate based: e.g. every 100ms</li>
 * </ol>
 *
 * <h2>Frequency based logging</h2>
 * With frequency based logging each timestep thread will add a log entry every so many calls. Frequency based logging can be
 * configured using:
 * <pre>
 * {@code
 *     class=yourtest
 *     logFrequency=10000
 * }
 * </pre>
 * Meaning that each timestep thread will log an entry every 10000th calls.
 *
 * Frequency based logging can be configured per execution group. If there is an execution group producer, then the logFrequency
 * can be configured using:
 * <pre>
 * {@code
 *     class=yourtest
 *     producerLogFrequency=10000
 * }
 * </pre>
 *
 * <h2>Time rate based logging</h2>
 * The time rate based logging allows for a log entry per timestep thread to be written at a maximum rate. E.g. if we want to see
 * at most 1 time a second a log entry, we can configure the test:
 * <pre>
 * {@code
 *     class=yourtest
 *     logRateMs=1000
 * }
 * </pre>
 * Time rate based logging is very useful to prevent overloading the system with log entries.
 *
 * Time rate based logging can be configured per execution group. If there is an execution group producer, then the logRate can
 * be configured using:
 * <pre>
 * {@code
 *     class=yourtest
 *     producerRateMs=1000
 * }
 * </pre>
 *
 * <h1>Latency testing</h1>
 * For Latency testing you normally want to rate the number of requests per second. This can be done by setting the interval
 * property. This property configures the interval between requests and is independent of thread count. So if interval is set
 * to 10ms, you get 100 operations/second. If there are 2 threads, each thread will do 1 request every 20ms. If there are
 * 4 threads, each thread will do 1 request every 40ms.
 *
 * Example:
 * <pre>
 * {@code
 *     class=yourtest
 *     interval=10ms
 *     threadCount=2
 * }
 * </pre>
 * In this example there will be 2 threads, that together will make 1 request every 10ms (so after 1 second, 100 requests have
 * been made). Interval can be configured with ns, us, ms, s, m, h. Keep in mind that the interval is per machine, so if the
 * interval is 10ms and there are 2 machines, on average there is 1 request every 5ms.
 *
 * The interval can also be configured using the ratePerSecond property:
 * <pre>
 * {@code
 *     class=yourtest
 *     ratePerSecond=100
 *     threadCount=2
 * }
 * </pre>
 * It is converted to interval under the hood, so there is no difference at runtime. ratePerSecond, just like interval, is
 * independent of the number of threads. If you have 200 requests per second, and 2 threads, each thread is going to do
 * 100 requests/second. With 4 threads, each thread is going to do 50 requests/second.
 *
 * If there are multiple execution groups, the interval can be configured using:
 * <pre>
 * {@code
 *     class=yourtest
 *     producerInterval=10ms
 *     producerThreadCount=2
 * }
 * </pre>
 *
 * <h2>Coordinated omission</h2>
 * A lot of testing frameworks are suffering from a problem called coordinated omission:
 * https://www.infoq.com/presentations/latency-pitfalls
 * By default coordinated omission is prevented by determining the latency based on the expected start-time instead of the actual
 * start time. This is all done under the hood and not something you need to worry about. In some cases you want to see the
 * impact of coordinated omission and you can allow for it using:
 * <pre>
 * {@code
 *     class=yourtest
 *     interval=10ms
 *     accountForCoordinatedOmission=false
 * }
 * </pre>
 *
 * <h2>Different flavors of metronomes</h2>
 * Internally a {@link com.hazelcast.simulator.worker.metronome.Metronome} is used to control the rate of requests. There are
 * currently 3 out of the box implementations:
 * <ol>
 * <li>{@link com.hazelcast.simulator.worker.metronome.SleepingMetronome}: which used LockSupport.park for waiting.
 * This metronome is the default and useful if you don't want to consume a lot of CPU cycles.</li>
 * <li>{@link com.hazelcast.simulator.worker.metronome.BusySpinningMetronome}: which used busy spinning for waiting.
 * It will give you the best time, but it will totally consume a single core. You certainly don't want to use this
 * metronome when having many load generating threads.
 * </li>
 * <li>{@link com.hazelcast.simulator.worker.metronome.ConstantCombinedRateMetronome} is special type of metronome. The
 * first 2 metronomes have a rate per thread; if a thread gets blocked, a bubble of requests will build up that needs
 * to get processed as soon as the thread unblocks. Even though coordinated omission by default is taken care of, the bubble
 * might not what you want because it means that you will get a dip in system pressure and then a peek, which
 * both can influence the benchmark. With the ConstantCombinedRateMetronome as long as their is a thread available, a
 * requests will be made. THis prevents building up the bubble and will give a more stable request rate.
 * </li>
 * </ol>
 *
 * The metronome type can be configured using:
 * <pre>
 * {@code
 *     class=yourtest
 *     interval=10ms
 *     metronomeClass=com.hazelcast.simulator.worker.metronome.ConstantCombinedRateMetronome
 * }
 * </pre>
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
