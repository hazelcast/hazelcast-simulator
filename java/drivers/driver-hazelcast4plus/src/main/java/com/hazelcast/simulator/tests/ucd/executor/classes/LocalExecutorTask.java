/*
 * Copyright (c) 2008-2024, Hazelcast, Inc. All Rights Reserved.
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
package com.hazelcast.simulator.tests.ucd.executor.classes;

import java.io.Serializable;
import java.util.concurrent.Callable;

public class LocalExecutorTask implements Callable<Long>, Serializable {
    private static final long serialVersionUID = 8301151618785236415L;
    private final long startTime;

    public LocalExecutorTask(long startTime) {
        this.startTime = startTime;
    }

    @Override
    public Long call() {
        return startTime;
    }
}
