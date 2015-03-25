package com.hazelcast.simulator.probes.probes.impl;

import com.hazelcast.simulator.probes.probes.ProbesResultXmlElements;
import com.hazelcast.simulator.probes.probes.Result;
import org.HdrHistogram.Histogram;
import org.apache.commons.codec.binary.Base64;

import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.ByteBuffer;

public class HdrLatencyDistributionResult implements Result<HdrLatencyDistributionResult> {

    public static final String XML_TYPE = HdrLatencyDistributionResult.class.getSimpleName();

    private final Histogram histogram;

    public HdrLatencyDistributionResult(Histogram histogram) {
        this.histogram = histogram;
    }

    public Histogram getHistogram() {
        return histogram;
    }

    @Override
    public HdrLatencyDistributionResult combine(HdrLatencyDistributionResult other) {
        if (other == null) {
            return this;
        }
        Histogram histogramCopy = new Histogram(histogram);
        histogramCopy.add(other.histogram);
        return new HdrLatencyDistributionResult(histogramCopy);
    }

    @Override
    public String toHumanString() {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        PrintStream stream = new PrintStream(outputStream);
        histogram.outputPercentileDistribution(stream, 1.0);
        stream.flush();
        return new String(outputStream.toByteArray());
    }

    @Override
    public void writeTo(XMLStreamWriter writer) {
        int size = histogram.getNeededByteBufferCapacity();
        ByteBuffer byteBuffer = ByteBuffer.allocate(size);
        int bytesWritten = histogram.encodeIntoCompressedByteBuffer(byteBuffer);
        byteBuffer.rewind();
        byteBuffer.limit(bytesWritten);
        String encodedData = Base64.encodeBase64String(byteBuffer.array());
        try {
            writer.writeStartElement(ProbesResultXmlElements.HDR_LATENCY_DATA.string);
            writer.writeCData(encodedData);
            writer.writeEndElement();
        } catch (XMLStreamException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        HdrLatencyDistributionResult that = (HdrLatencyDistributionResult) o;

        if (histogram != null ? !histogram.equals(that.histogram) : that.histogram != null) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        return histogram != null ? histogram.hashCode() : 0;
    }
}
