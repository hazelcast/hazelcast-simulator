package com.hazelcast.simulator.utils;

import org.junit.Test;

import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;
import java.io.Closeable;
import java.io.IOException;
import java.net.Socket;

import static com.hazelcast.simulator.utils.CommonUtils.closeQuietly;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

public class CommonUtils_CloseableTest {

    @Test
    public void testCloseQuietlySocketNull() {
        closeQuietly((Socket) null);
    }

    @Test
    public void testCloseQuietlySocket() {
        Socket socket = mock(Socket.class);
        closeQuietly(socket);
    }

    @Test
    public void testCloseQuietlySocketException() throws IOException {
        Socket socket = mock(Socket.class);
        doThrow(new IOException("Expected exception")).when(socket).close();
        closeQuietly(socket);
    }

    @Test
    public void testCloseQuietlyCloseableNull() {
        closeQuietly((Closeable) null);
    }

    @Test
    public void testCloseQuietlyCloseable() {
        Closeable closeable = mock(Closeable.class);
        closeQuietly(closeable);
    }

    @Test
    public void testCloseQuietlyCloseableException() throws IOException {
        Closeable closeable = mock(Closeable.class);
        doThrow(new IOException("Expected exception")).when(closeable).close();
        closeQuietly(closeable);
    }

    @Test
    public void testCloseQuietlyCloseablesNull() {
        closeQuietly(null, null);
    }

    @Test
    public void testCloseQuietlyCloseables() {
        Closeable closeable = mock(Closeable.class);
        closeQuietly(closeable, closeable);
    }

    @Test
    public void testCloseQuietlyCloseablesException() throws IOException {
        Closeable closeable = mock(Closeable.class);
        Closeable closeableException = mock(Closeable.class);
        doThrow(new IOException("Expected exception")).when(closeableException).close();
        closeQuietly(closeable, closeableException);
    }

    @Test
    public void testCloseQuietlyXMLStreamWriterNull() {
        closeQuietly((XMLStreamWriter) null);
    }

    @Test
    public void testCloseQuietlyXMLStreamWriter() {
        XMLStreamWriter xmlStreamWriter = mock(XMLStreamWriter.class);
        closeQuietly(xmlStreamWriter);
    }

    @Test
    public void testCloseQuietlyXMLStreamWriterException() throws XMLStreamException {
        XMLStreamWriter xmlStreamWriter = mock(XMLStreamWriter.class);
        doThrow(new XMLStreamException("Expected exception")).when(xmlStreamWriter).close();
        closeQuietly(xmlStreamWriter);
    }
}
