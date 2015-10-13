package com.hazelcast.simulator.probes.xml;

import com.hazelcast.simulator.probes.Result;
import com.hazelcast.simulator.probes.impl.ResultImpl;
import com.thoughtworks.xstream.converters.ConversionException;
import org.junit.AfterClass;
import org.junit.Test;

import static com.hazelcast.simulator.probes.ProbeTestUtils.assertEqualsResult;
import static com.hazelcast.simulator.probes.ProbeTestUtils.cleanup;
import static com.hazelcast.simulator.probes.ProbeTestUtils.createProbeResult;
import static com.hazelcast.simulator.probes.ProbeTestUtils.getResultFile;
import static com.hazelcast.simulator.probes.ProbeTestUtils.serializeAndDeserializeAgain;
import static com.hazelcast.simulator.probes.xml.ResultXmlUtils.fromXml;
import static com.hazelcast.simulator.probes.xml.ResultXmlUtils.toXml;
import static com.hazelcast.simulator.utils.FileUtils.writeText;
import static com.hazelcast.simulator.utils.ReflectionUtils.invokePrivateConstructor;
import static org.apache.commons.codec.binary.Base64.encodeBase64String;

public class ResultXmlUtilsTest {

    @AfterClass
    public static void tearDown() {
        cleanup();
    }

    @Test
    public void constructorReader() throws Exception {
        invokePrivateConstructor(ResultXmlUtils.class);
    }

    @Test
    public void testProbeResult() {
        Result expected = createProbeResult(1);
        Result actual = serializeAndDeserializeAgain(expected);
        assertEqualsResult(expected, actual);
    }

    @Test
    public void testProbeResult_multipleProbes() {
        Result expected = createProbeResult(3);
        Result actual = serializeAndDeserializeAgain(expected);
        assertEqualsResult(expected, actual);
    }

    @Test
    public void testProbeResult_emptyResult() {
        Result expected = createProbeResult(0);
        Result actual = serializeAndDeserializeAgain(expected);
        assertEqualsResult(expected, actual);
    }

    @Test(expected = ConversionException.class)
    public void testProbeResult_invalidHistogramXml() {
        Result result = new ResultImpl("InvalidHistogramXmlTest", 1000, 500.0);

        String xml = toXml(result);
        String invalidHistogram = encodeBase64String("invalid".getBytes());
        String invalidXml = xml.replace(
                "<histograms/>",
                "<histograms><name>probeName</name><data>" + invalidHistogram + "</data></histograms>");
        writeText(invalidXml, getResultFile());

        fromXml(getResultFile());
    }
}
