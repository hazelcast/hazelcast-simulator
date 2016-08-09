package com.hazelcast.simulator.utils;

import org.junit.Test;

import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;
import java.io.Closeable;
import java.io.IOException;
import java.net.Socket;
import java.util.jar.JarFile;

import static com.hazelcast.simulator.utils.CommonUtils.closeQuietly;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

public class CommonUtils_CloseableTest {

    @Test
    public void testCloseQuietlySocketNull() {
        closeQuietly((Socket) null);
    }


    @Test
    public void testCloseQuietlyCloseableNull() {
        closeQuietly((Closeable) null);
    }

    @Test
    public void testCloseQuietlyCloseable() throws Exception {
        Closeable closeable = mock(Closeable.class);
        closeQuietly(closeable);

        verify(closeable).close();
        verifyNoMoreInteractions(closeable);
    }

    @Test
    public void testCloseQuietlyCloseableException() throws Exception {
        Closeable closeable = mock(Closeable.class);
        doThrow(new IOException("Expected exception")).when(closeable).close();

        closeQuietly(closeable);
    }

    @Test
    public void testCloseQuietlyCloseablesNull() {
        closeQuietly(null, null);
    }

    @Test
    public void testCloseQuietlyCloseables() throws Exception {
        Closeable closeable = mock(Closeable.class);

        closeQuietly(closeable, closeable);

        verify(closeable, times(2)).close();
        verifyNoMoreInteractions(closeable);
    }

    @Test
    public void testCloseQuietlyCloseablesException() throws Exception {
        Closeable closeable = mock(Closeable.class);
        Closeable closeableException = mock(Closeable.class);
        doThrow(new IOException("Expected exception")).when(closeableException).close();

        closeQuietly(closeable, closeableException);
    }

    @Test
    public void testCloseQuietlyJarFileNull() {
        closeQuietly((JarFile) null);
    }

    @Test
    public void testCloseQuietlyJarFile() throws Exception {
        JarFile jarFile = mock(JarFile.class);
        closeQuietly(jarFile);

        verify(jarFile).close();
        verifyNoMoreInteractions(jarFile);
    }

    @Test
    public void testCloseQuietlyJarFileException() throws Exception {
        JarFile jarFile = mock(JarFile.class);
        doThrow(new IOException("Expected exception")).when(jarFile).close();

        closeQuietly(jarFile);
    }
}
