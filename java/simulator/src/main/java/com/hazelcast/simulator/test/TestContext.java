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
package com.hazelcast.simulator.test;

/**
 * The TestContext is they way for a test to get access to test related information. Most importantly if a test is running.
 */
public interface TestContext {

    /**
     * Returns the id of the current test.
     *
     * @return the id of the current test.
     */
    String getTestId();

    /**
     * Returns the public ip address of the machine the test runs on. In some environments like ec2, there are public and private
     * ip addresses.
     *
     * @return the public ip address.
     */
    String getPublicIpAddress();

    /**
     * Checks if the run phase or warmup phase has stopped. In most cases this method doesn't need to be called since the
     * {@link com.hazelcast.simulator.test.annotations.TimeStep} approach will take care of stopping. But in certain cases
     * like using the {@link com.hazelcast.simulator.test.annotations.Run}; one needs to check explicitly.
     *
     * @return true if stopped, false otherwise.
     */
    boolean isStopped();

    /**
     * Stops the run or warmup phase. In most cases an outside duration is passed and the test will run as long as needed or
     * until an exception is thrown. But in certain condition the implementer of a test wants to stop the run/warmup phase
     * directly.
     *
     * Once stopped, the test moves on to the next phase. If the warmup is stopped, the test will eventually move on to the
     * run phase.
     */
    void stop();

    /**
     * Echoes a message to coordinator.
     *
     * Be very careful sending huge quantities of messages to the coordinator because it cause stability issues. Messages are
     * written async, so you could easily kill the by flooding it or causing other problems. So don't use this as a debug logging
     * alternative.
     *
     * @param msg the message to send
     * @param args the arguments
     */
    void echoCoordinator(String msg, Object... args);
}
