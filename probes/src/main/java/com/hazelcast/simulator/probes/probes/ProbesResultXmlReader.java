package com.hazelcast.simulator.probes.probes;

import com.hazelcast.simulator.probes.probes.impl.HdrLatencyDistributionResult;
import com.hazelcast.simulator.probes.probes.impl.LatencyDistributionResult;
import com.hazelcast.simulator.probes.probes.impl.MaxLatencyResult;
import com.hazelcast.simulator.probes.probes.impl.OperationsPerSecResult;
import org.HdrHistogram.Histogram;
import org.apache.commons.codec.binary.Base64;

import javax.xml.namespace.QName;
import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.events.EndElement;
import javax.xml.stream.events.StartElement;
import javax.xml.stream.events.XMLEvent;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.DataFormatException;

import static com.hazelcast.simulator.probes.probes.ProbesResultXmlElements.HDR_LATENCY_DATA;
import static com.hazelcast.simulator.probes.probes.ProbesResultXmlElements.INVOCATIONS;
import static com.hazelcast.simulator.probes.probes.ProbesResultXmlElements.LATENCY_DIST_BUCKET;
import static com.hazelcast.simulator.probes.probes.ProbesResultXmlElements.LATENCY_DIST_BUCKETS;
import static com.hazelcast.simulator.probes.probes.ProbesResultXmlElements.LATENCY_DIST_MAX_VALUE;
import static com.hazelcast.simulator.probes.probes.ProbesResultXmlElements.LATENCY_DIST_STEP;
import static com.hazelcast.simulator.probes.probes.ProbesResultXmlElements.LATENCY_DIST_UPPER_BOUND;
import static com.hazelcast.simulator.probes.probes.ProbesResultXmlElements.LATENCY_DIST_VALUES;
import static com.hazelcast.simulator.probes.probes.ProbesResultXmlElements.MAX_LATENCY;
import static com.hazelcast.simulator.probes.probes.ProbesResultXmlElements.OPERATIONS_PER_SECOND;
import static com.hazelcast.simulator.probes.probes.ProbesResultXmlElements.PROBE;
import static com.hazelcast.simulator.probes.probes.ProbesResultXmlElements.PROBES_RESULT;
import static com.hazelcast.simulator.probes.probes.ProbesResultXmlElements.PROBE_NAME;
import static com.hazelcast.simulator.probes.probes.ProbesResultXmlElements.PROBE_TYPE;

public final class ProbesResultXmlReader {

    private ProbesResultXmlReader() {
    }

    public static Map<String, Result> read(InputStream inputStream) {
        Map<String, Result> result = new HashMap<String, Result>();
        XMLInputFactory xmlInputFactory = XMLInputFactory.newInstance();
        try {
            XMLEventReader reader = xmlInputFactory.createXMLEventReader(inputStream);
            while (reader.hasNext()) {
                XMLEvent event = reader.nextEvent();
                if (event.isStartElement()) {
                    StartElement startElement = event.asStartElement();
                    if (PROBES_RESULT.string.equals(startElement.getName().getLocalPart())) {
                        parseProbesResult(reader, result);
                    }
                }
            }
        } catch (XMLStreamException e) {
            e.printStackTrace();
        }
        return result;
    }

    private static void parseProbesResult(XMLEventReader reader, Map<String, Result> result) throws XMLStreamException {
        while (reader.hasNext()) {
            XMLEvent event = reader.nextEvent();
            if (event.isStartElement()) {
                StartElement startElement = event.asStartElement();
                if (PROBE.string.equals(startElement.getName().getLocalPart())) {
                    parseProbe(reader, startElement, result);
                }
            } else if (event.isEndElement()) {
                EndElement endElement = event.asEndElement();
                if (!PROBES_RESULT.string.equals(endElement.getName().getLocalPart())) {
                    throw new XMLStreamException("Unexpected end element " + endElement.getName());
                }
                return;
            }
        }
    }

    private static void parseProbe(XMLEventReader reader, StartElement startElement, Map<String, Result> result)
            throws XMLStreamException {
        String name = startElement.getAttributeByName(new QName(PROBE_NAME.string)).getValue();
        String type = startElement.getAttributeByName(new QName(PROBE_TYPE.string)).getValue();

        Result probeResult = null;
        if (OperationsPerSecResult.XML_TYPE.equals(type)) {
            probeResult = parseOperationsPerSecResult(reader);
        } else if (MaxLatencyResult.XML_TYPE.equals(type)) {
            probeResult = parseMaxLatencyResult(reader);
        } else if (HdrLatencyDistributionResult.XML_TYPE.equals(type)) {
            probeResult = parseHdrLatencyProbeResult(reader);
        } else if (LatencyDistributionResult.XML_TYPE.equals(type)) {
            probeResult = parseLatencyDistributionResult(reader);
        }
        result.put(name, probeResult);

        while (reader.hasNext()) {
            XMLEvent eventType = reader.nextEvent();
            if (eventType.isEndElement()) {
                EndElement endElement = eventType.asEndElement();
                if (!PROBE.string.equals(endElement.getName().getLocalPart())) {
                    throw new XMLStreamException("Unexpected end element " + endElement.getName());
                }
                return;
            } else if (eventType.isStartElement()) {
                throw new XMLStreamException("Unexpected start element " + eventType.asStartElement().getName());
            } else if (eventType.isCharacters()) {
                throw new XMLStreamException("Unexpected characters " + eventType.asCharacters().getData());
            }
        }
    }

    private static Result parseOperationsPerSecResult(XMLEventReader reader) throws XMLStreamException {
        Long invocations = null;
        Double operationsPerSecond = null;

        while (reader.hasNext()) {
            XMLEvent xmlEvent = reader.nextEvent();
            if (xmlEvent.isStartElement()) {
                StartElement startElement = xmlEvent.asStartElement();
                if (INVOCATIONS.string.equals(startElement.getName().getLocalPart())) {
                    if (invocations != null) {
                        throw new XMLStreamException("Unexpected element " + INVOCATIONS.string + " (has been already defined)");
                    }
                    invocations = Long.parseLong(parseCharsAndEndCurrentElement(reader));
                } else if (OPERATIONS_PER_SECOND.string.equals(startElement.getName().getLocalPart())) {
                    if (operationsPerSecond != null) {
                        throw new XMLStreamException(
                                "Unexpected element " + OPERATIONS_PER_SECOND.string + " (has been already defined)");
                    }
                    operationsPerSecond = Double.parseDouble(parseCharsAndEndCurrentElement(reader));
                }
            }
        }
        if (invocations != null && operationsPerSecond != null) {
            return new OperationsPerSecResult(invocations, operationsPerSecond);
        }
        throw new XMLStreamException("Unexpected end of stream");
    }

    private static MaxLatencyResult parseMaxLatencyResult(XMLEventReader reader) throws XMLStreamException {
        Long maxLatency = null;
        while (reader.hasNext()) {
            XMLEvent xmlEvent = reader.nextEvent();
            if (xmlEvent.isCharacters()) {
                maxLatency = Long.parseLong(xmlEvent.asCharacters().getData());
            } else if (xmlEvent.isEndElement()) {
                EndElement endElement = xmlEvent.asEndElement();
                if (!MAX_LATENCY.string.equals(endElement.getName().getLocalPart())) {
                    throw new XMLStreamException("Unexpected end element " + endElement.getName());
                }
                if (maxLatency == null) {
                    throw new XMLStreamException("Unexpected end element " + MAX_LATENCY.string);
                }
                return new MaxLatencyResult(maxLatency);
            }
        }
        throw new XMLStreamException("Unexpected end of stream");
    }

    private static Result parseHdrLatencyProbeResult(XMLEventReader reader) throws XMLStreamException {
        String encodedData = null;
        while (reader.hasNext()) {
            XMLEvent xmlEvent = reader.nextEvent();
            if (xmlEvent.isCharacters()) {
                encodedData = xmlEvent.asCharacters().getData();
            } else if (xmlEvent.isEndElement()) {
                EndElement endElement = xmlEvent.asEndElement();
                if (!HDR_LATENCY_DATA.string.equals(endElement.getName().getLocalPart())) {
                    throw new XMLStreamException("Unexpected end element " + endElement.getName());
                }
                if (encodedData == null) {
                    throw new XMLStreamException("Unexpected end element " + HDR_LATENCY_DATA.string);
                }
                try {
                    byte[] bytes = Base64.decodeBase64(encodedData);
                    Histogram histogram = Histogram.decodeFromCompressedByteBuffer(ByteBuffer.wrap(bytes), 0);
                    return new HdrLatencyDistributionResult(histogram);
                } catch (DataFormatException e) {
                    throw new RuntimeException(e);
                }
            }
        }
        throw new XMLStreamException("Unexpected end of stream");
    }

    private static LatencyDistributionResult parseLatencyDistributionResult(XMLEventReader reader) throws XMLStreamException {
        Integer step = null;
        Integer maxValue = null;

        while (reader.hasNext()) {
            XMLEvent xmlEvent = reader.nextEvent();
            if (xmlEvent.isStartElement()) {
                StartElement startElement = xmlEvent.asStartElement();
                if (LATENCY_DIST_STEP.string.equals(startElement.getName().getLocalPart())) {
                    if (step != null) {
                        throw new XMLStreamException("Unexpected element " + LATENCY_DIST_STEP.string);
                    }
                    step = Integer.parseInt(parseCharsAndEndCurrentElement(reader));
                } else if (LATENCY_DIST_MAX_VALUE.string.equals(startElement.getName().getLocalPart())) {
                    if (maxValue != null) {
                        throw new XMLStreamException("Unexpected element " + LATENCY_DIST_MAX_VALUE.string);
                    }
                    maxValue = Integer.parseInt(parseCharsAndEndCurrentElement(reader));
                } else if (LATENCY_DIST_BUCKETS.string.equals(startElement.getName().getLocalPart())) {
                    if (step == null || maxValue == null) {
                        throw new XMLStreamException("Unexpected element " + LATENCY_DIST_BUCKETS.string);
                    }
                    LinearHistogram histogram = new LinearHistogram(maxValue, step);
                    parseBuckets(reader, histogram);
                    return new LatencyDistributionResult(histogram);
                }
            }
        }
        throw new XMLStreamException("Unexpected end of the document");
    }

    private static void parseBuckets(XMLEventReader reader, LinearHistogram histogram) throws XMLStreamException {
        while (reader.hasNext()) {
            XMLEvent xmlEvent = reader.nextEvent();
            if (xmlEvent.isStartElement()) {
                StartElement startElement = xmlEvent.asStartElement();
                if (LATENCY_DIST_BUCKET.string.equals(startElement.getName().getLocalPart())) {
                    parseBucket(reader, startElement, histogram);
                }
            } else if (xmlEvent.isEndElement()) {
                EndElement endElement = xmlEvent.asEndElement();
                if (!LATENCY_DIST_BUCKETS.string.equals(endElement.getName().getLocalPart())) {
                    throw new XMLStreamException("Unexpected end element " + endElement.getName());
                }
                return;
            }
        }
    }

    private static void parseBucket(XMLEventReader reader, StartElement element, LinearHistogram histogram)
            throws XMLStreamException {
        String upperBound = element.getAttributeByName(new QName(LATENCY_DIST_UPPER_BOUND.string)).getValue();
        String values = element.getAttributeByName(new QName(LATENCY_DIST_VALUES.string)).getValue();
        histogram.addMultipleValues(Integer.parseInt(upperBound) - 1, Integer.parseInt(values));

        while (reader.hasNext()) {
            XMLEvent xmlEvent = reader.nextEvent();
            if (xmlEvent.isEndElement()) {
                EndElement endElement = xmlEvent.asEndElement();
                if (LATENCY_DIST_BUCKET.string.equals(endElement.getName().getLocalPart())) {
                    return;
                }
            }
        }
        throw new XMLStreamException("Unexpected end of the document");
    }

    private static String parseCharsAndEndCurrentElement(XMLEventReader reader) throws XMLStreamException {
        XMLEvent xmlEvent = reader.nextEvent();
        if (!xmlEvent.isCharacters()) {
            throw new XMLStreamException("Unexpected event " + xmlEvent);
        }
        String data = xmlEvent.asCharacters().getData();
        while (reader.hasNext()) {
            xmlEvent = reader.nextEvent();
            if (xmlEvent.isEndElement()) {
                return data;
            }
        }
        throw new XMLStreamException("Unexpected end of the document");
    }
}
