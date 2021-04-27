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
package com.hazelcast.simulator.tests.icache.helpers;

import java.io.Serializable;

public final class ICacheListenerOperationCounter implements Serializable {

    public long register;
    public long registerIllegalArgException;
    public long deRegister;
    public long put;
    public long get;

    public void add(ICacheListenerOperationCounter counter) {
        register += counter.register;
        registerIllegalArgException += counter.registerIllegalArgException;
        deRegister += counter.deRegister;
        put += counter.put;
        get += counter.get;
    }

    public String toString() {
        return "Counter{"
                + "register=" + register
                + ", registerIllegalArgException=" + registerIllegalArgException
                + ", deRegister=" + deRegister
                + ", put=" + put
                + ", get=" + get
                + '}';
    }
}
