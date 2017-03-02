package com.hazelcast.simulator.worker.testcontainer;

import com.hazelcast.simulator.common.TestCase;
import com.hazelcast.simulator.utils.compiler.InMemoryJavaCompiler;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class TimeStepModel_probabilityTest {

    @Test
    public void test_probability_singleMethod() {
        HashMap<String, Double> probs = new HashMap<String, Double>();
        TimeStepModel model = loadModel("public class CLAZZ{\n"
                + "@TimeStep public void timeStep1(){}\n"
                + "}\n", probs);

        assertProbability(model, "timeStep1", 1.0);
        assertNull(model.getTimeStepProbabilityArray(""));
    }

    private void assertProbability(TimeStepModel model, String method, double value) {
        assertEquals(value, model.getProbability("", method).getValue(), 0.001);
    }

    private void assertProbability(TimeStepModel model, String group, String method, double value) {
        assertEquals(value, model.getProbability(group, method).getValue(), 0.001);
    }

    @Test(expected = IllegalTestException.class)
    public void test_probability_probabilityUsingAnnotationExceedsOne() {
        HashMap<String, Double> probs = new HashMap<String, Double>();

        loadModel("public class CLAZZ{\n"
                + "@TimeStep(prob=0.5) public void timeStep1(){}\n"
                + "@TimeStep(prob=0.6) public void timeStep2(){}\n"
                + "}\n", probs);
    }

    @Test(expected = IllegalTestException.class)
    public void test_probability_multipleDefaultMethodsUsingAnnotation() {
        HashMap<String, Double> probs = new HashMap<String, Double>();

        loadModel("public class CLAZZ{\n"
                + "@TimeStep(prob=-1) public void timeStep1(){}\n"
                + "@TimeStep(prob=-1) public void timeStep2(){}\n"
                + "}\n", probs);
    }

    @Test(expected = IllegalTestException.class)
    public void test_probability_multipleDefaultMethodsUsingExternalConfig() {
        HashMap<String, Double> probs = new HashMap<String, Double>();
        probs.put("timeStep1Prob", -1d);
        probs.put("timeStep2Prob", -1d);

        loadModel("public class CLAZZ{\n"
                + "@TimeStep(prob=0.5) public void timeStep1(){}\n"
                + "@TimeStep(prob=0.5) public void timeStep2(){}\n"
                + "}\n", probs);
    }

    @Test(expected = IllegalTestException.class)
    public void test_probability_probabilityUsingExternalConfigExceedsOne() {
        HashMap<String, Double> probs = new HashMap<String, Double>();
        probs.put("timeStep1Prob", 1d);
        probs.put("timeStep2Prob", 1d);

        loadModel("public class CLAZZ{\n"
                + "@TimeStep(prob=0.5) public void timeStep1(){}\n"
                + "@TimeStep(prob=0.5) public void timeStep2(){}\n"
                + "}\n", probs);
    }

    @Test
    public void test_singleActiveMethodUsingExternalProperties() {
        HashMap<String, Double> probs = new HashMap<String, Double>();
        probs.put("timeStep1Prob", 1d);
        probs.put("timeStep2Prob", 0d);

        TimeStepModel model = loadModel("public class CLAZZ{\n"
                + "@TimeStep(prob=0.5) public void timeStep1(){}\n"
                + "@TimeStep(prob=0.5) public void timeStep2(){}\n"
                + "}\n", probs);

        assertProbability(model, "timeStep1", 1.0);
        assertProbability(model, "timeStep2", 0.0);
        assertNull(model.getTimeStepProbabilityArray(""));
    }

    @Test
    public void test_singleActiveMethodProperties_multipleProbabilities() {
        HashMap<String, Double> probs = new HashMap<String, Double>();

        TimeStepModel model = loadModel("public class CLAZZ{\n"
                + "@TimeStep(prob=0.10, executionGroup=\"a\") public void a1(){}\n"
                + "@TimeStep(prob=0.90, executionGroup=\"a\") public void a2(){}\n"
                + "@TimeStep(prob=0.20, executionGroup=\"b\") public void b1(){}\n"
                + "@TimeStep(prob=0.80, executionGroup=\"b\") public void b2(){}\n"

                + "}\n", probs);

        assertProbability(model, "a", "a1", 0.10);
        assertProbability(model, "a", "a2", 0.90);
        assertProbability(model, "b", "b1", 0.20);
        assertProbability(model, "b", "b2", 0.80);

        assertNotNull(model.getTimeStepProbabilityArray("a"));
        assertNotNull(model.getTimeStepProbabilityArray("b"));
    }

    @Test
    public void test_singleActiveMethodExternalProperties_multipleProbabilities() {
        HashMap<String, Double> probs = new HashMap<String, Double>();
        probs.put("a1Prob", 0.05);
        probs.put("a2Prob", 0.95);
        probs.put("b1Prob", 0.03);
        probs.put("b2Prob", 0.97);

        TimeStepModel model = loadModel("public class CLAZZ{\n"
                + "@TimeStep(prob=0.10, executionGroup=\"a\") public void a1(){}\n"
                + "@TimeStep(prob=0.90, executionGroup=\"a\") public void a2(){}\n"
                + "@TimeStep(prob=0.20, executionGroup=\"b\") public void b1(){}\n"
                + "@TimeStep(prob=0.80, executionGroup=\"b\") public void b2(){}\n"
                + "}\n", probs);

        assertProbability(model, "a", "a1", 0.05);
        assertProbability(model, "a", "a2", 0.95);
        assertProbability(model, "b", "b1", 0.03);
        assertProbability(model, "b", "b2", 0.97);

        assertNotNull(model.getTimeStepProbabilityArray("a"));
        assertNotNull(model.getTimeStepProbabilityArray("b"));
    }

    @Test
    public void testThousandProbability() {
        HashMap<String, Double> probs = new HashMap<String, Double>();
        probs.put("aProb", 0.001);
        probs.put("bProb", 0.999);

        TimeStepModel model = loadModel("public class CLAZZ{\n"
                + "@TimeStep(prob=0.001) public void a(){}\n"
                + "@TimeStep(prob=0.999) public void b(){}\n"
                + "}\n", probs);

        assertProbability(model, "a", 0.001);
        assertProbability(model, "b", 0.999);

        assertNotNull(model.getTimeStepProbabilityArray(""));
    }

    @Test
    public void testTenThousandProbability() {
        HashMap<String, Double> probs = new HashMap<String, Double>();

        TimeStepModel model = loadModel("public class CLAZZ{\n"
                + "@TimeStep(prob=0.0001) public void a(){}\n"
                + "@TimeStep(prob=0.9999) public void b(){}\n"
                + "}\n", probs);

        assertProbability(model, "a", 0.0001);
        assertProbability(model, "b", 0.9999);

        assertNotNull(model.getTimeStepProbabilityArray(""));
    }

    @Test
    public void testHundredThousandProbability() {
        HashMap<String, Double> probs = new HashMap<String, Double>();

        TimeStepModel model = loadModel("public class CLAZZ{\n"
                + "@TimeStep(prob=0.00001) public void a(){}\n"
                + "@TimeStep(prob=0.99999) public void b(){}\n"
                + "}\n", probs);

        assertProbability(model, "a", 0.00001);
        assertProbability(model, "b", 0.99999);

        assertNotNull(model.getTimeStepProbabilityArray(""));
    }

    @Test
    public void testMillionProbability() {
        HashMap<String, Double> probs = new HashMap<String, Double>();

        TimeStepModel model = loadModel("public class CLAZZ{\n"
                + "@TimeStep(prob=0.000001) public void a(){}\n"
                + "@TimeStep(prob=0.999999) public void b(){}\n"
                + "}\n", probs);

        assertProbability(model, "a", 0.000001);
        assertProbability(model, "b", 0.999999);

        assertNotNull(model.getTimeStepProbabilityArray(""));
    }

    @Test
    public void test_singleActiveMethodUsingAnnotationSettings() {
        HashMap<String, Double> probs = new HashMap<String, Double>();

        TimeStepModel model = loadModel("public class CLAZZ{\n"
                + "@TimeStep(prob=1) public void timeStep1(){}\n"
                + "@TimeStep(prob=0) public void timeStep2(){}\n"
                + "}\n", probs);

        assertProbability(model, "timeStep1", 1.0);
        assertProbability(model, "timeStep2", 0.0);
        assertNull(model.getTimeStepProbabilityArray(""));
    }



    @Test
    public void test_multipleActiveMethods() {
        HashMap<String, Double> probs = new HashMap<String, Double>();

        TimeStepModel model = loadModel("public class CLAZZ{\n"
                + "@TimeStep(prob=0.5) public void timeStep1(){}\n"
                + "@TimeStep(prob=0.5) public void timeStep2(){}\n"
                + "}\n", probs);

        assertProbability(model, "timeStep1", 0.5);
        assertProbability(model, "timeStep2", 0.5);
        assertNotNull(model.getTimeStepProbabilityArray(""));
    }

    @Test
    public void test_defaultMethod() {
        HashMap<String, Double> probs = new HashMap<String, Double>();

        TimeStepModel model = loadModel("public class CLAZZ{\n"
                + "@TimeStep(prob=0.2) public void timeStep1(){}\n"
                + "@TimeStep(prob=-1) public void timeStep2(){}\n"
                + "}\n", probs);

        assertProbability(model, "timeStep1", 0.2);
        assertProbability(model, "timeStep2", 0.8);
        assertNotNull(model.getTimeStepProbabilityArray(""));
    }

    @Test
    public void test_defaultMethod_andExternalConfiguration() {
        HashMap<String, Double> probs = new HashMap<String, Double>();
        probs.put("timeStep1Prob", 0.3);
        probs.put("timeStep2Prob", -1.0);

        TimeStepModel model = loadModel("public class CLAZZ{\n"
                + "@TimeStep(prob=0.2) public void timeStep1(){}\n"
                + "@TimeStep(prob=0.8) public void timeStep2(){}\n"
                + "}\n", probs);

        assertProbability(model, "timeStep1", 0.3);
        assertProbability(model, "timeStep2", 0.7);
        assertNotNull(model.getTimeStepProbabilityArray(""));
    }

    private TimeStepModel loadModel(String code, Map<String, Double> probs) {
        String header = "import java.util.*;\n"
                + "import com.hazelcast.simulator.test.annotations.*;\n"
                + "import com.hazelcast.simulator.test.annotations.*;\n";
        code = header + code;
        String className = "CLAZZ" + UUID.randomUUID().toString().replace("-", "");
        code = code.replace("CLAZZ", className);

        Class clazz;
        try {
            clazz = InMemoryJavaCompiler.compile(className, code);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        TestCase testCase = new TestCase("foo").setProperty("class", clazz);
        for (Map.Entry<String, Double> entry : probs.entrySet()) {
            testCase.setProperty(entry.getKey(), entry.getValue());
        }
        return new TimeStepModel(clazz, new PropertyBinding(testCase));
    }
}
