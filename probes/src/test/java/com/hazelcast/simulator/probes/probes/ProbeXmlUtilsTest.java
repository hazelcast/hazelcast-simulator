package com.hazelcast.simulator.probes.probes;

import com.hazelcast.simulator.probes.probes.impl.ResultImpl;
import com.thoughtworks.xstream.converters.ConversionException;
import org.junit.AfterClass;
import org.junit.Test;

import static com.hazelcast.simulator.probes.probes.ProbeXmlUtils.fromXml;
import static com.hazelcast.simulator.probes.probes.ProbeXmlUtils.toXml;
import static com.hazelcast.simulator.probes.probes.utils.ResultTestUtils.assertEqualsResult;
import static com.hazelcast.simulator.probes.probes.utils.ResultTestUtils.cleanup;
import static com.hazelcast.simulator.probes.probes.utils.ResultTestUtils.createProbeResult;
import static com.hazelcast.simulator.probes.probes.utils.ResultTestUtils.getResultFile;
import static com.hazelcast.simulator.probes.probes.utils.ResultTestUtils.serializeAndDeserializeAgain;
import static com.hazelcast.simulator.utils.FileUtils.writeText;
import static com.hazelcast.simulator.utils.ReflectionUtils.invokePrivateConstructor;
import static org.apache.commons.codec.binary.Base64.encodeBase64String;

public class ProbeXmlUtilsTest {

    @AfterClass
    public static void tearDown() {
        cleanup();
    }

    @Test
    public void constructorReader() throws Exception {
        invokePrivateConstructor(ProbeXmlUtils.class);
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
