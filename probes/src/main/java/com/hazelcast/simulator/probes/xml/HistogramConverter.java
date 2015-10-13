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
