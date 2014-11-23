package com.hazelcast.stabilizer.probes.probes.impl;

import com.hazelcast.stabilizer.probes.probes.Result;
import org.HdrHistogram.Histogram;
import sun.misc.BASE64Encoder;

import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.ByteBuffer;

public class HdrLatencyProbeResult implements Result<HdrLatencyProbeResult> {
    private final Histogram histogram;

    public HdrLatencyProbeResult(Histogram histogram) {
        this.histogram = histogram;
    }

    @Override
    public HdrLatencyProbeResult combine(HdrLatencyProbeResult other) {
        if (other == null) {
            return this;
        }
        Histogram histogramCopy = new Histogram(histogram);
        histogramCopy.add(other.histogram);
        return new HdrLatencyProbeResult(histogramCopy);
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
        BASE64Encoder encoder = new BASE64Encoder();
        String encodedData = encoder.encode(byteBuffer);
        try {
            writer.writeStartElement("data");
            writer.writeCData(encodedData);
            writer.writeEndElement();
        } catch (XMLStreamException e) {
            new RuntimeException(e);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        HdrLatencyProbeResult that = (HdrLatencyProbeResult) o;

        if (histogram != null ? !histogram.equals(that.histogram) : that.histogram != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return histogram != null ? histogram.hashCode() : 0;
    }

    public Histogram getHistogram() {
        return histogram;
    }
}
