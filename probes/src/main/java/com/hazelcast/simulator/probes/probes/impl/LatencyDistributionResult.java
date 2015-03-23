package com.hazelcast.simulator.probes.probes.impl;

import com.hazelcast.simulator.probes.probes.LinearHistogram;
import com.hazelcast.simulator.probes.probes.ProbesResultXmlElements;
import com.hazelcast.simulator.probes.probes.Result;

import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;
import java.text.NumberFormat;
import java.util.Locale;

public class LatencyDistributionResult implements Result<LatencyDistributionResult> {

    public static final String XML_TYPE = LatencyDistributionResult.class.getSimpleName();

    private final LinearHistogram linearHistogram;

    public LatencyDistributionResult(LinearHistogram linearHistogram) {
        this.linearHistogram = linearHistogram;
    }

    @Override
    public LatencyDistributionResult combine(LatencyDistributionResult other) {
        if (other == null) {
            return this;
        }
        return new LatencyDistributionResult(linearHistogram.combine(other.linearHistogram));
    }

    @Override
    public String toHumanString() {
        StringBuilder builder = new StringBuilder();
        NumberFormat numberFormat = NumberFormat.getInstance(Locale.US);
        int numberOfSpaces = 15;
        double percentiles[] = {0.999d, 0.99d, 0.9d, 0.85d, 0.8d, 0.75d, 0.7d, 0.6d, 0.5d, 0.4d, 0.3d};
        builder.append('\n');
        for (double percentile : percentiles) {
            HistogramPart value = linearHistogram.getPercentile(percentile);
            String formattedPercentile = numberFormat.format(percentile);
            builder.append("Percentile ")
                    .append(formattedPercentile).append(" ")
                    .append(String.format("%" + (numberOfSpaces - formattedPercentile.length()) + "s", value.getBucket()))
                    .append(" µs")
                    .append(String.format("%" + numberOfSpaces + "s", value.getValues()))
                    .append(" ops\n");
        }
        return builder.toString();
    }

    public LinearHistogram getHistogram() {
        return linearHistogram;
    }

    @Override
    public void writeTo(XMLStreamWriter writer) {
        try {
            writer.writeStartElement(ProbesResultXmlElements.LATENCY_DIST_STEP.string);
            int step = linearHistogram.getStep();
            writer.writeCharacters(Integer.toString(step));
            writer.writeEndElement();

            writer.writeStartElement(ProbesResultXmlElements.LATENCY_DIST_MAX_VALUE.string);
            int maxValue = linearHistogram.getMaxValue();
            writer.writeCharacters(Integer.toString(maxValue));
            writer.writeEndElement();

            writer.writeStartElement(ProbesResultXmlElements.LATENCY_DIST_BUCKETS.string);
            int[] buckets = linearHistogram.getBuckets();
            for (int i = 0; i < buckets.length; i++) {
                int upperBound = (i + 1) * step;
                int values = buckets[i];
                if (values != 0) {
                    writer.writeStartElement(ProbesResultXmlElements.LATENCY_DIST_BUCKET.string);
                    writer.writeAttribute(ProbesResultXmlElements.LATENCY_DIST_UPPER_BOUND.string, Integer.toString(upperBound));
                    writer.writeAttribute(ProbesResultXmlElements.LATENCY_DIST_VALUES.string, Integer.toString(values));
                    writer.writeEndElement();
                }
            }
            writer.writeEndElement();
        } catch (XMLStreamException e) {
            throw new IllegalStateException("Error while writing probe output", e);
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

        LatencyDistributionResult that = (LatencyDistributionResult) o;

        if (!linearHistogram.equals(that.linearHistogram)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        return linearHistogram.hashCode();
    }
}
