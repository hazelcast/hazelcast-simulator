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

import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.util.Map;

import static com.hazelcast.simulator.probes.probes.ProbesResultXmlElements.PROBE;
import static com.hazelcast.simulator.probes.probes.ProbesResultXmlElements.PROBES_RESULT;
import static com.hazelcast.simulator.probes.probes.ProbesResultXmlElements.PROBE_NAME;
import static com.hazelcast.simulator.utils.CommonUtils.closeQuietly;

public final class ProbesResultXmlWriter {

    private ProbesResultXmlWriter() {
    }

    public static void write(Map<String, Result> combinedResults, File file) {
        FileOutputStream fos = null;
        try {
            fos = new FileOutputStream(file);
            write(combinedResults, fos);
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        } finally {
            closeQuietly(fos);
        }
    }

    public static void write(Map<String, Result> combinedResults, OutputStream outputStream) {
        XMLOutputFactory xmlOutputFactory = XMLOutputFactory.newInstance();
        XMLStreamWriter xmlStreamWriter = null;
        try {
            xmlStreamWriter = xmlOutputFactory.createXMLStreamWriter(outputStream);
            xmlStreamWriter.writeStartDocument();
            xmlStreamWriter.writeStartElement(PROBES_RESULT.getName());
            for (Map.Entry<String, Result> entry : combinedResults.entrySet()) {
                String probeName = entry.getKey();
                Result result = entry.getValue();
                xmlStreamWriter.writeStartElement(PROBE.getName());
                xmlStreamWriter.writeAttribute(PROBE_NAME.getName(), probeName);
                result.writeTo(xmlStreamWriter);
                xmlStreamWriter.writeEndElement();
            }
            xmlStreamWriter.writeEndDocument();
        } catch (XMLStreamException e) {
            throw new RuntimeException("Cannot create XML Output Stream Writer");
        } finally {
            closeQuietly(xmlStreamWriter);
        }
    }
}
