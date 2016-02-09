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
package com.hazelcast.simulator.probes.xml;

import com.thoughtworks.xstream.converters.Converter;
import com.thoughtworks.xstream.converters.MarshallingContext;
import com.thoughtworks.xstream.converters.UnmarshallingContext;
import com.thoughtworks.xstream.io.HierarchicalStreamReader;
import com.thoughtworks.xstream.io.HierarchicalStreamWriter;
import org.HdrHistogram.Histogram;

import java.nio.ByteBuffer;

import static org.HdrHistogram.Histogram.decodeFromCompressedByteBuffer;
import static org.apache.commons.codec.binary.Base64.decodeBase64;
import static org.apache.commons.codec.binary.Base64.encodeBase64String;

final class HistogramConverter implements Converter {

    @Override
    public boolean canConvert(Class clazz) {
        return clazz.equals(Histogram.class);
    }

    @Override
    public void marshal(Object object, HierarchicalStreamWriter writer, MarshallingContext marshallingContext) {
        Histogram histogram = (Histogram) object;
        int size = histogram.getNeededByteBufferCapacity();
        ByteBuffer byteBuffer = ByteBuffer.allocate(size);
        int bytesWritten = histogram.encodeIntoCompressedByteBuffer(byteBuffer);
        byteBuffer.rewind();
        byteBuffer.limit(bytesWritten);
        String encodedHistogram = encodeBase64String(byteBuffer.array());

        writer.setValue(encodedHistogram);
    }

    @Override
    public Object unmarshal(HierarchicalStreamReader reader, UnmarshallingContext unmarshallingContext) {
        String encodedHistogram = reader.getValue();

        try {
            byte[] bytes = decodeBase64(encodedHistogram);
            return decodeFromCompressedByteBuffer(ByteBuffer.wrap(bytes), 0);
        } catch (Exception e) {
            throw new IllegalArgumentException("Could not parse histogram", e);
        }
    }
}
