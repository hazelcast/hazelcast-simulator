/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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
package com.hazelcast.simulator.probes.probes;

import com.hazelcast.simulator.probes.probes.impl.HdrResult;
import com.hazelcast.simulator.probes.probes.impl.ThroughputResult;
import org.HdrHistogram.Histogram;
import org.apache.commons.codec.binary.Base64;
import org.apache.log4j.Logger;

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
import static com.hazelcast.simulator.probes.probes.ProbesResultXmlElements.OPERATIONS_PER_SECOND;
import static com.hazelcast.simulator.probes.probes.ProbesResultXmlElements.PROBE;
import static com.hazelcast.simulator.probes.probes.ProbesResultXmlElements.PROBES_RESULT;
import static com.hazelcast.simulator.probes.probes.ProbesResultXmlElements.PROBE_NAME;
import static com.hazelcast.simulator.probes.probes.ProbesResultXmlElements.PROBE_TYPE;

public final class ProbesResultXmlReader {

    private static final Logger LOGGER = Logger.getLogger(ProbesResultXmlReader.class);

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
                    if (PROBES_RESULT.matches(startElement.getName().getLocalPart())) {
                        parseProbesResult(reader, result);
                    }
                }
            }
        } catch (XMLStreamException e) {
            LOGGER.error("Error while reading XML probe result stream", e);
        }
        return result;
    }

    private static void parseProbesResult(XMLEventReader reader, Map<String, Result> result) throws XMLStreamException {
        while (reader.hasNext()) {
            XMLEvent event = reader.nextEvent();
            if (event.isStartElement()) {
                StartElement startElement = event.asStartElement();
                if (PROBE.matches(startElement.getName().getLocalPart())) {
                    parseProbe(reader, startElement, result);
                }
            } else if (event.isEndElement()) {
                EndElement endElement = event.asEndElement();
                boolean isProbeEnd = PROBE.matches(endElement.getName().getLocalPart());
                boolean isProbeResultEnd = PROBES_RESULT.matches(endElement.getName().getLocalPart());
                if (!isProbeEnd && !isProbeResultEnd) {
                    throw new XMLStreamException("Unexpected end element " + endElement.getName());
                }
                if (isProbeResultEnd) {
                    return;
                }
            }
        }
    }

    private static void parseProbe(XMLEventReader reader, StartElement startElement, Map<String, Result> result)
            throws XMLStreamException {
        String name = startElement.getAttributeByName(new QName(PROBE_NAME.getName())).getValue();
        String type = startElement.getAttributeByName(new QName(PROBE_TYPE.getName())).getValue();

        Result probeResult = null;
        if (ThroughputResult.XML_TYPE.equals(type)) {
            probeResult = parseOperationsPerSecResult(reader);
        } else if (HdrResult.XML_TYPE.equals(type)) {
            probeResult = parseHdrLatencyProbeResult(reader);
        }
        result.put(name, probeResult);
    }

    private static Result parseOperationsPerSecResult(XMLEventReader reader) throws XMLStreamException {
        Long invocations = null;
        Double operationsPerSecond = null;

        while (reader.hasNext()) {
            XMLEvent xmlEvent = reader.nextEvent();
            if (xmlEvent.isStartElement()) {
                StartElement startElement = xmlEvent.asStartElement();
                if (INVOCATIONS.matches(startElement.getName().getLocalPart())) {
                    if (invocations != null) {
                        throw new XMLStreamException("Unexpected element " + INVOCATIONS.getName()
                                + " (has been already defined)");
                    }
                    invocations = Long.parseLong(parseCharsAndEndCurrentElement(reader));
                } else if (OPERATIONS_PER_SECOND.matches(startElement.getName().getLocalPart())) {
                    if (operationsPerSecond != null) {
                        throw new XMLStreamException(
                                "Unexpected element " + OPERATIONS_PER_SECOND.getName() + " (has been already defined)");
                    }
                    operationsPerSecond = Double.parseDouble(parseCharsAndEndCurrentElement(reader));
                }
                if (invocations != null && operationsPerSecond != null) {
                    return new ThroughputResult(invocations, operationsPerSecond);
                }
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
                if (!HDR_LATENCY_DATA.matches(endElement.getName().getLocalPart()) || encodedData == null) {
                    throw new XMLStreamException("Unexpected end element " + endElement.getName());
                }
                try {
                    byte[] bytes = Base64.decodeBase64(encodedData);
                    Histogram histogram = Histogram.decodeFromCompressedByteBuffer(ByteBuffer.wrap(bytes), 0);
                    return new HdrResult(histogram);
                } catch (DataFormatException e) {
                    throw new RuntimeException(e);
                }
            }
        }
        throw new XMLStreamException("Unexpected end of stream");
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
