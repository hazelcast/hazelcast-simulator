package com.hazelcast.stabilizer.probes.probes;

import com.hazelcast.stabilizer.probes.probes.LinearHistogram;
import com.hazelcast.stabilizer.probes.probes.Result;
import com.hazelcast.stabilizer.probes.probes.impl.LatencyDistributionResult;
import com.hazelcast.stabilizer.probes.probes.impl.MaxLatencyResult;
import com.hazelcast.stabilizer.probes.probes.impl.OperationsPerSecondResult;

import javax.xml.namespace.QName;
import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.events.EndElement;
import javax.xml.stream.events.StartElement;
import javax.xml.stream.events.XMLEvent;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

public class ProbesResultXmlReader {

    public Map<String, Result> read(InputStream inputStream) {
        Map<String, Result> result = new HashMap<String, Result>();
        XMLInputFactory xmlInputFactory = XMLInputFactory.newInstance();
        try {
            XMLEventReader reader = xmlInputFactory.createXMLEventReader(inputStream);
            while (reader.hasNext()) {
                XMLEvent event = reader.nextEvent();
                if (event.isStartElement()) {
                    StartElement startElement = event.asStartElement();
                    if ("probes-result".equals(startElement.getName().getLocalPart())) {
                        parseProbesResult(reader, result);
                    }
                }
            }
        } catch (XMLStreamException e) {
            e.printStackTrace();
        }
        return result;
    }

    private void parseProbesResult(XMLEventReader reader, Map<String, Result> result) throws XMLStreamException {
        while (reader.hasNext()) {
            XMLEvent event = reader.nextEvent();
            if (event.isEndElement()) {
                EndElement endElement = event.asEndElement();
                if ("probes-result".equals(endElement.getName().getLocalPart())) {
                    return;
                } else {
                    throw new XMLStreamException("Unexpected end element "+endElement);
                }
            } else if (event.isStartElement()) {
                StartElement startElement = event.asStartElement();
                if ("probe".equals(startElement.getName().getLocalPart())) {
                    parseProbe(reader, startElement, result);
                }
            }
        }
    }

    private void parseProbe(XMLEventReader reader, StartElement startElement, Map<String, Result> result) throws XMLStreamException {
        String name = startElement.getAttributeByName(new QName("name")).getValue();
        String type = startElement.getAttributeByName(new QName("type")).getValue();

        Result probeResult = null;
        if ("MaxLatencyResult".equals(type)) {
            probeResult = parseMaxLatencyResult(reader);
        } else if ("LatencyDistributionResult".equals(type)) {
            probeResult = parseLatencyDistributionResult(reader);
        } else if ("OperationsPerSecondResult".equals(type)) {
            probeResult = parseOperationsPerSecondResult(reader);
        }
        result.put(name, probeResult);

        while (reader.hasNext()) {
            XMLEvent eventType = reader.nextEvent();
            if (eventType.isEndElement()) {
                EndElement endElement = eventType.asEndElement();
                if ("probe".equals(endElement.getName().getLocalPart())) {
                    return;
                } else {
                    throw new XMLStreamException("Unexpected end element "+endElement.getName());
                }
            } else if (eventType.isStartElement()) {
                throw new XMLStreamException("Unexpected start element "+eventType.asStartElement().getName());
            } else if (eventType.isCharacters()) {
                throw new XMLStreamException("Unexpected characters "+eventType.asCharacters().getData());
            }
        }
    }

    private Result parseOperationsPerSecondResult(XMLEventReader reader) throws XMLStreamException {
        Double operationsPerSecond = null;
        while (reader.hasNext()) {
            XMLEvent xmlEvent = reader.nextEvent();
            if (xmlEvent.isEndElement()) {
                EndElement endElement = xmlEvent.asEndElement();
                if ("operations-per-second".equals(endElement.getName().getLocalPart())) {
                    if (operationsPerSecond!= null) {
                        return new OperationsPerSecondResult(operationsPerSecond);
                    } else {
                        throw new XMLStreamException("Unexpected end element operations-per-second.");
                    }
                } else {
                    throw new XMLStreamException("Unexpected end element "+endElement.getName());
                }
            } else if (xmlEvent.isCharacters()) {
                String data = xmlEvent.asCharacters().getData();
                operationsPerSecond = Double.parseDouble(data);
            }
        }
        throw new XMLStreamException("Unexpected end of stream");
    }

    private LatencyDistributionResult parseLatencyDistributionResult(XMLEventReader reader) throws XMLStreamException {
        LinearHistogram histogram = null;
        Integer step = null;
        Integer maxValue = null;

        while (reader.hasNext()) {
            XMLEvent xmlEvent = reader.nextEvent();
            if (xmlEvent.isStartElement()) {
                StartElement startElement = xmlEvent.asStartElement();
                if ("step".equals(startElement.getName().getLocalPart())) {
                    if (step == null) {
                        step = Integer.parseInt(parseCharsAndEndCurrentElement(reader));
                        if (maxValue != null) {
                            histogram = new LinearHistogram(maxValue, step);
                        }
                    } else {
                        throw new XMLStreamException("Unexpected element step. Step has been already defined");
                    }
                } else if ("max-value".equals(startElement.getName().getLocalPart())) {
                    if (maxValue == null) {
                        maxValue = Integer.parseInt(parseCharsAndEndCurrentElement(reader));
                        if (step != null) {
                            histogram = new LinearHistogram(maxValue, step);
                        }
                    } else {
                        throw new XMLStreamException("Unexpected element max-value. Max-value has been already defined");
                    }
                } else if ("buckets".equals(startElement.getName().getLocalPart())) {
                    parseBuckets(reader, histogram);
                    return new LatencyDistributionResult(histogram);
                }
            }
        }
        throw new XMLStreamException("Unexpected end of the document");
    }

    private void parseBuckets(XMLEventReader reader, LinearHistogram histogram) throws XMLStreamException {
        while (reader.hasNext()) {
            XMLEvent xmlEvent = reader.nextEvent();
            if (xmlEvent.isEndElement()) {
                EndElement endElement = xmlEvent.asEndElement();
                if ("buckets".equals(endElement.getName().getLocalPart())) {
                    return;
                } else {
                    throw new XMLStreamException("Unexpected end element "+endElement.getName());
                }
            } else if (xmlEvent.isStartElement()) {
                StartElement startElement = xmlEvent.asStartElement();
                if ("bucket".equals(startElement.getName().getLocalPart())) {
                    parseBucket(reader, startElement, histogram);
                }
            }
        }
    }

    private void parseBucket(XMLEventReader reader, StartElement element, LinearHistogram histogram) throws XMLStreamException {
        String upperBound = element.getAttributeByName(new QName("upper-bound")).getValue();
        String values = element.getAttributeByName(new QName("values")).getValue();
        histogram.addMultipleValues(Integer.parseInt(upperBound)-1, Integer.parseInt(values));

        while (reader.hasNext()) {
            XMLEvent xmlEvent = reader.nextEvent();
            if (xmlEvent.isEndElement()) {
                EndElement endElement = xmlEvent.asEndElement();
                if ("bucket".equals(endElement.getName().getLocalPart())) {
                    return;
                }
            }
        }
        throw new XMLStreamException("Unexpected end of the document");
    }

    private String parseCharsAndEndCurrentElement(XMLEventReader reader) throws XMLStreamException {
        XMLEvent xmlEvent = reader.nextEvent();
        if (!xmlEvent.isCharacters()) {
            throw new XMLStreamException("Unexpected event "+xmlEvent);
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

    private MaxLatencyResult parseMaxLatencyResult(XMLEventReader reader) throws XMLStreamException {
        Long maxLatency = null;
        while (reader.hasNext()) {
            XMLEvent xmlEvent = reader.nextEvent();
            if (xmlEvent.isEndElement()) {
                EndElement endElement = xmlEvent.asEndElement();
                if ("max-latency".equals(endElement.getName().getLocalPart())) {
                    if (maxLatency != null) {
                        return new MaxLatencyResult(maxLatency);
                    } else {
                        throw new XMLStreamException("Unexpected end element max-latency.");
                    }
                } else {
                    throw new XMLStreamException("Unexpected end element "+endElement.getName());
                }
            } else if (xmlEvent.isCharacters()) {
                String data = xmlEvent.asCharacters().getData();
                maxLatency = Long.parseLong(data);
            }
        }
        throw new XMLStreamException("Unexpected end of stream");
    }

    private enum Elements {
        PROBE_RESULT,
        PROBE,
        STEP,
        BUCKET,
        OPERATIONS_PER_SECOND,
        MAX_LATENCY
    }

}
