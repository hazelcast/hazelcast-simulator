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
package com.hazelcast.simulator.hz;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

public class ExternalizablePojo implements Externalizable {
    private int id;

    public ExternalizablePojo() {
        // No-op.
    }

    public ExternalizablePojo(int id) {
        this.id = id;
    }

    public int id() {
        return id;
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException {
        id = in.readInt();
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeInt(id);
    }

    @Override
    public String toString() {
        return "ExternalizablePojo{id=" + id + '}';
    }
}
