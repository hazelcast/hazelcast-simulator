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

public class ICacheCreateDestroyCounter implements Serializable {

    public long put;
    public long create;
    public long close;
    public long destroy;

    public long putException;
    public long createException;
    public long closeException;
    public long destroyException;

    public void add(ICacheCreateDestroyCounter c) {
        put += c.put;
        create += c.create;
        close += c.close;
        destroy += c.destroy;

        putException += c.putException;
        createException += c.createException;
        closeException += c.closeException;
        destroyException += c.destroyException;
    }

    @Override
    public String toString() {
        return "Counter{"
                + "put=" + put
                + ", create=" + create
                + ", close=" + close
                + ", destroy=" + destroy
                + ", putException=" + putException
                + ", createException=" + createException
                + ", closeException=" + closeException
                + ", destroyException=" + destroyException
                + '}';
    }
}
