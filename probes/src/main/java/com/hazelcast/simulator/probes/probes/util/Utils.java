package com.hazelcast.simulator.probes.probes.util;

import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;
import java.io.Closeable;
import java.io.IOException;

// this class can be removed once util methods are in a separate module
public final class Utils {

    private Utils() {
    }

    public static void closeQuietly(XMLStreamWriter c) {
        if (c == null) {
            return;
        }
        try {
            c.close();
        } catch (XMLStreamException ignore) {
        }
    }

    public static void closeQuietly(Closeable c) {
        if (c == null) {
            return;
        }
        try {
            c.close();
        } catch (IOException ignore) {
        }
    }

}
