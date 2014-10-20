package com.hazelcast.stabilizer.probes.probes;

import com.hazelcast.stabilizer.probes.probes.util.Utils;

import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.util.Map;

public class ProbesResultXmlWriter {

    public <R extends Result<R>> void write(Map<String, R> combinedResults, OutputStream outputStream) {
        XMLOutputFactory xmlOutputFactory = XMLOutputFactory.newInstance();
        XMLStreamWriter xmlStreamWriter = null;
        try {
            xmlStreamWriter = xmlOutputFactory.createXMLStreamWriter(outputStream);
            xmlStreamWriter.writeStartDocument();
            xmlStreamWriter.writeStartElement("probes-result");
            for (Map.Entry<String, R> entry : combinedResults.entrySet()) {
                String probeName = entry.getKey();
                R result = entry.getValue();
                xmlStreamWriter.writeStartElement("probe");
                xmlStreamWriter.writeAttribute("name", probeName);
                xmlStreamWriter.writeAttribute("type", result.getClass().getSimpleName());
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

    public <R extends Result<R>> void write(Map<String, R> combinedResults, File file) {
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
}
