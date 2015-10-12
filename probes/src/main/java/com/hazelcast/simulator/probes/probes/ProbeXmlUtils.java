package com.hazelcast.simulator.probes.probes;

import com.hazelcast.simulator.probes.probes.impl.ResultImpl;
import com.thoughtworks.xstream.XStream;
import com.thoughtworks.xstream.converters.Converter;
import com.thoughtworks.xstream.converters.MarshallingContext;
import com.thoughtworks.xstream.converters.UnmarshallingContext;
import com.thoughtworks.xstream.converters.extended.NamedMapConverter;
import com.thoughtworks.xstream.io.HierarchicalStreamReader;
import com.thoughtworks.xstream.io.HierarchicalStreamWriter;
import com.thoughtworks.xstream.io.xml.DomDriver;
import com.thoughtworks.xstream.mapper.Mapper;
import org.HdrHistogram.Histogram;

import java.io.File;
import java.nio.ByteBuffer;

import static com.hazelcast.simulator.utils.FileUtils.writeText;
import static org.HdrHistogram.Histogram.decodeFromCompressedByteBuffer;
import static org.apache.commons.codec.binary.Base64.decodeBase64;
import static org.apache.commons.codec.binary.Base64.encodeBase64String;

public final class ProbeXmlUtils {

    private ProbeXmlUtils() {
    }

    public static String toXml(Result result) {
        XStream xStream = getXStream();
        return xStream.toXML(result);
    }

    public static void toXml(Result result, File file) {
        writeText(toXml(result), file);
    }

    public static Result fromXml(File file) {
        XStream xStream = getXStream();
        return (Result) xStream.fromXML(file);
    }

    private static XStream getXStream() {
        XStream xStream = new XStream(new DomDriver());
        Mapper mapper = xStream.getMapper();

        xStream.registerConverter(new HistogramConverter());
        xStream.registerConverter(new NamedMapConverter(mapper, null, "name", String.class, "data", Histogram.class));

        xStream.alias("probeResult", ResultImpl.class);
        xStream.alias("histogram", Histogram.class);
        xStream.aliasField("histograms", ResultImpl.class, "probeHistogramMap");

        return xStream;
    }

    private static final class HistogramConverter implements Converter {

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
                throw new RuntimeException("Could not parse histogram");
            }
        }
    }
}
