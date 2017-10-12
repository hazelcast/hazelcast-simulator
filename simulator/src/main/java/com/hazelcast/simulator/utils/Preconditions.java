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
package com.hazelcast.simulator.utils;

public final class Preconditions {

    private Preconditions() {
    }

    /**
     * Tests if an argument is not {@code null}.
     *
     * @param argument     the argument tested to see if it is not {@code null}.
     * @param errorMessage the errorMessage
     * @param <T>          type of the argument
     * @return the argument if its not {@code null}
     * @throws java.lang.NullPointerException if argument is {@code null}
     */
    public static <T> T checkNotNull(T argument, String errorMessage) {
        if (argument == null) {
            throw new NullPointerException(errorMessage);
        }
        return argument;
    }
}
