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

/**
 * This class does nothing!
 *
 * It is useful if you e.g. don't need to do anything with an exception; but checkstyle is complaining that you need to have
 * at least one statement.
 */
public final class EmptyStatement {

    private EmptyStatement() {
    }

    /**
     * Does totally nothing.
     *
     * @param ignored the exception to ignore
     */
    @SuppressWarnings("unused")
    public static void ignore(Throwable ignored) {
    }
}
