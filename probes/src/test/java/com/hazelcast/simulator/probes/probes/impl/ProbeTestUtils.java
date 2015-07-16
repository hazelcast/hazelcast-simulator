package com.hazelcast.simulator.probes.probes.impl;

import com.hazelcast.simulator.probes.probes.Result;
import com.hazelcast.simulator.probes.probes.SimpleProbe;
import com.hazelcast.simulator.utils.EmptyStatement;

import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;

import static java.lang.String.format;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

public class ProbeTestUtils {

    static final int TOLERANCE_MILLIS = 50;

    private static final Integer WRONG_CLASS = 42;

    static void assertDisable(SimpleProbe probe) {
        assertFalse(probe.isDisabled());

        probe.disable();

        assertTrue(probe.isDisabled());
    }

    static void assertResult(Result result) {
        assertResult(result, null);
    }

    @SuppressWarnings("all")
    static void assertResult(Result result, Result notEqualsResult) {
        assertNotNull(result);
        assertNotNull(result.toHumanString());

        assertEquals(result, result.combine(null));
        assertFalse(result.equals(null));
        assertFalse(result.equals(WRONG_CLASS));

        assertNotEquals(result, notEqualsResult);

        try {
            XMLStreamWriter writer = mock(XMLStreamWriter.class);
            doThrow(new XMLStreamException()).when(writer).writeStartElement(anyString());

            result.writeTo(writer);
            fail("Expected RuntimeException due to XMLStreamException");
        } catch (XMLStreamException e) {
            fail(e.getMessage());
        } catch (RuntimeException e) {
            // expected exception
            EmptyStatement.ignore(e);
        }
    }

    static void assertWithinTolerance(String fieldName, long expected, long actual, long tolerance) {
        assertTrue(format("Expected %s >= %d, but was %d", fieldName, expected - tolerance, actual),
                actual >= expected - tolerance);
        assertTrue(format("Expected %s <= %d, but was %d", fieldName, expected + tolerance, actual),
                actual <= expected + tolerance);
    }
}
