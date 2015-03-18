package com.hazelcast.simulator.probes.probes;

import com.hazelcast.simulator.probes.probes.util.Utils;

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
import static com.hazelcast.simulator.probes.probes.ProbesResultXmlElements.PROBE_TYPE;

public class ProbesResultXmlWriter {

    public static <R extends Result<R>> void write(Map<String, R> combinedResults, File file) {
        FileOutputStream fos = null;
        try {
            fos = new FileOutputStream(file);
            write(combinedResults, fos);
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        } finally {
            Utils.closeQuietly(fos);
        }
    }

    public static <R extends Result<R>> void write(Map<String, R> combinedResults, OutputStream outputStream) {
        XMLOutputFactory xmlOutputFactory = XMLOutputFactory.newInstance();
        XMLStreamWriter xmlStreamWriter = null;
        try {
            xmlStreamWriter = xmlOutputFactory.createXMLStreamWriter(outputStream);
            xmlStreamWriter.writeStartDocument();
            xmlStreamWriter.writeStartElement(PROBES_RESULT.string);
            for (Map.Entry<String, R> entry : combinedResults.entrySet()) {
                String probeName = entry.getKey();
                R result = entry.getValue();
                xmlStreamWriter.writeStartElement(PROBE.string);
                xmlStreamWriter.writeAttribute(PROBE_NAME.string, probeName);
                xmlStreamWriter.writeAttribute(PROBE_TYPE.string, result.getClass().getSimpleName());
                result.writeTo(xmlStreamWriter);
                xmlStreamWriter.writeEndElement();
            }
            xmlStreamWriter.writeEndDocument();
        } catch (XMLStreamException e) {
            throw new RuntimeException("Cannot create XML Output Stream Writer");
        } finally {
            Utils.closeQuietly(xmlStreamWriter);
        }
    }
}
