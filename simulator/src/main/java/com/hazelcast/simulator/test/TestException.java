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
package com.hazelcast.simulator.test;

import static java.lang.String.format;

/**
 * Exception for failures in Simulator tests.
 *
 * Should be used instead of a {@link RuntimeException}.
 */
public class TestException extends RuntimeException {

    public TestException(Throwable cause) {
        super(cause);
    }

    public TestException(String message) {
        super(message);
    }

    public TestException(String message, Object... args) {
        super(format(message, args));

        Object lastArg = args[args.length - 1];
        if (lastArg instanceof Throwable) {
            initCause((Throwable) lastArg);
        }
    }
}
