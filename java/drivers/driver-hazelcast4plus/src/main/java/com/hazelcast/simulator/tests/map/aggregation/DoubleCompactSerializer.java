/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
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
package com.hazelcast.simulator.tests.map.aggregation;

import com.hazelcast.nio.serialization.compact.CompactReader;
import com.hazelcast.nio.serialization.compact.CompactSerializer;
import com.hazelcast.nio.serialization.compact.CompactWriter;
import org.jetbrains.annotations.NotNull;

public class DoubleCompactSerializer implements CompactSerializer<DoubleCompactPojo> {
    @NotNull
    @Override
    public DoubleCompactPojo read(@NotNull CompactReader compactReader) {
        DoubleCompactPojo pojo = new DoubleCompactPojo();
        pojo.n1 = compactReader.readFloat64("n1");
        pojo.n2 = compactReader.readFloat64("n2");
        pojo.n3 = compactReader.readFloat64("n3");
        pojo.n4 = compactReader.readFloat64("n4");
        return pojo;
    }

    @Override
    public void write(@NotNull CompactWriter compactWriter, @NotNull DoubleCompactPojo pojo) {
        compactWriter.writeFloat64("n1", pojo.n1);
        compactWriter.writeFloat64("n2", pojo.n2);
        compactWriter.writeFloat64("n3", pojo.n3);
        compactWriter.writeFloat64("n4", pojo.n4);
    }

    @NotNull
    @Override
    public String getTypeName() {
        return "doubleCompactPojo";
    }

    @NotNull
    @Override
    public Class<DoubleCompactPojo> getCompactClass() {
        return DoubleCompactPojo.class;
    }
}
