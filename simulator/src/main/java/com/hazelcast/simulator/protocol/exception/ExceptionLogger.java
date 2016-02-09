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
package com.hazelcast.simulator.protocol.exception;

/**
 * Logs exceptions to files or Simulator Coordinator, depending on the implementation.
 */
public interface ExceptionLogger {

    /**
     * Maximum number of exceptions which will be logged.
     */
    int MAX_EXCEPTION_COUNT = 1000;

    /**
     * Returns the number of log method invocations.
     *
     * This value can be higher than {@value #MAX_EXCEPTION_COUNT}.
     *
     * @return the log invocation count.
     */
    long getLogInvocationCount();

    /**
     * Logs an exception.
     *
     * @param cause the {@link Throwable} that should be logged.
     */
    void log(Throwable cause);

    /**
     * Logs an exception of a Simulator test.
     *
     * @param cause  the {@link Throwable} that should be logged.
     * @param testId the id of the test that caused the exception.
     */
    void log(Throwable cause, String testId);
}
