package com.hazelcast.simulator.worker.testcontainer;

import com.hazelcast.simulator.common.TestCase;
import com.hazelcast.simulator.utils.compiler.InMemoryJavaCompiler;
import org.junit.Test;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class TimeStepModel_probabilityTest {

    private static final double DOUBLE_TOLERANCE_DELTA = 0.001;

    private HashMap<String, Double> probabilities = new HashMap<String, Double>();

    @Test
    public void test_probability_singleMethod() {
        TimeStepModel model = loadModel("public class CLAZZ{\n"
                + "@TimeStep public void timeStep1(){}\n"
                + "}\n", probabilities);

        assertProbability(model, "timeStep1", 1.0);
        assertNull(model.getTimeStepProbabilityArray(""));
    }

    @Test(expected = IllegalTestException.class)
    public void test_probability_probabilityUsingAnnotationExceedsOne() {
        loadModel("public class CLAZZ{\n"
                + "@TimeStep(prob=0.5) public void timeStep1(){}\n"
                + "@TimeStep(prob=0.6) public void timeStep2(){}\n"
                + "}\n", probabilities);
    }

    @Test(expected = IllegalTestException.class)
    public void test_probability_multipleDefaultMethodsUsingAnnotation() {
        loadModel("public class CLAZZ{\n"
                + "@TimeStep(prob=-1) public void timeStep1(){}\n"
                + "@TimeStep(prob=-1) public void timeStep2(){}\n"
                + "}\n", probabilities);
    }

    @Test(expected = IllegalTestException.class)
    public void test_probability_multipleDefaultMethodsUsingExternalConfig() {
        probabilities.put("timeStep1Prob", -1d);
        probabilities.put("timeStep2Prob", -1d);

        loadModel("public class CLAZZ{\n"
                + "@TimeStep(prob=0.5) public void timeStep1(){}\n"
                + "@TimeStep(prob=0.5) public void timeStep2(){}\n"
                + "}\n", probabilities);
    }

    @Test(expected = IllegalTestException.class)
    public void test_probability_probabilityUsingExternalConfigExceedsOne() {
        probabilities.put("timeStep1Prob", 1d);
        probabilities.put("timeStep2Prob", 1d);

        loadModel("public class CLAZZ{\n"
                + "@TimeStep(prob=0.5) public void timeStep1(){}\n"
                + "@TimeStep(prob=0.5) public void timeStep2(){}\n"
                + "}\n", probabilities);
    }

    @Test
    public void test_singleActiveMethodUsingExternalProperties() {
        probabilities.put("timeStep1Prob", 1d);
        probabilities.put("timeStep2Prob", 0d);

        TimeStepModel model = loadModel("public class CLAZZ{\n"
                + "@TimeStep(prob=0.5) public void timeStep1(){}\n"
                + "@TimeStep(prob=0.5) public void timeStep2(){}\n"
                + "}\n", probabilities);

        assertProbability(model, "timeStep1", 1.0);
        assertProbability(model, "timeStep2", 0.0);
        assertNull(model.getTimeStepProbabilityArray(""));
    }

    @Test
    public void test_singleActiveMethodProperties_multipleProbabilities() {
        TimeStepModel model = loadModel("public class CLAZZ{\n"
                + "@TimeStep(prob=0.10, executionGroup=\"a\") public void a1(){}\n"
                + "@TimeStep(prob=0.90, executionGroup=\"a\") public void a2(){}\n"
                + "@TimeStep(prob=0.20, executionGroup=\"b\") public void b1(){}\n"
                + "@TimeStep(prob=0.80, executionGroup=\"b\") public void b2(){}\n"
                + "}\n", probabilities);

        assertProbability(model, "a", "a1", 0.10);
        assertProbability(model, "a", "a2", 0.90);
        assertProbability(model, "b", "b1", 0.20);
        assertProbability(model, "b", "b2", 0.80);

        assertNotNull(model.getTimeStepProbabilityArray("a"));
        assertNotNull(model.getTimeStepProbabilityArray("b"));
    }

    @Test
    public void test_singleActiveMethodExternalProperties_multipleProbabilities() {
        probabilities.put("a1Prob", 0.05);
        probabilities.put("a2Prob", 0.95);
        probabilities.put("b1Prob", 0.03);
        probabilities.put("b2Prob", 0.97);

        TimeStepModel model = loadModel("public class CLAZZ{\n"
                + "@TimeStep(prob=0.10, executionGroup=\"a\") public void a1(){}\n"
                + "@TimeStep(prob=0.90, executionGroup=\"a\") public void a2(){}\n"
                + "@TimeStep(prob=0.20, executionGroup=\"b\") public void b1(){}\n"
                + "@TimeStep(prob=0.80, executionGroup=\"b\") public void b2(){}\n"
                + "}\n", probabilities);

        assertProbability(model, "a", "a1", 0.05);
        assertProbability(model, "a", "a2", 0.95);
        assertProbability(model, "b", "b1", 0.03);
        assertProbability(model, "b", "b2", 0.97);

        assertNotNull(model.getTimeStepProbabilityArray("a"));
        assertNotNull(model.getTimeStepProbabilityArray("b"));
    }

    @Test
    public void test_singleActiveMethodUsingAnnotationSettings() {
        TimeStepModel model = loadModel("public class CLAZZ{\n"
                + "@TimeStep(prob=1) public void timeStep1(){}\n"
                + "@TimeStep(prob=0) public void timeStep2(){}\n"
                + "}\n", probabilities);

        assertProbability(model, "timeStep1", 1.0);
        assertProbability(model, "timeStep2", 0.0);
        assertNull(model.getTimeStepProbabilityArray(""));
    }

    @Test
    public void test_multipleActiveMethods() {
        TimeStepModel model = loadModel("public class CLAZZ{\n"
                + "@TimeStep(prob=0.5) public void timeStep1(){}\n"
                + "@TimeStep(prob=0.5) public void timeStep2(){}\n"
                + "}\n", probabilities);

        assertProbability(model, "timeStep1", 0.5);
        assertProbability(model, "timeStep2", 0.5);
        assertNotNull(model.getTimeStepProbabilityArray(""));
    }

    @Test
    public void test_defaultMethod() {
        TimeStepModel model = loadModel("public class CLAZZ{\n"
                + "@TimeStep(prob=0.2) public void timeStep1(){}\n"
                + "@TimeStep(prob=-1) public void timeStep2(){}\n"
                + "}\n", probabilities);

        assertProbability(model, "timeStep1", 0.2);
        assertProbability(model, "timeStep2", 0.8);
        assertNotNull(model.getTimeStepProbabilityArray(""));
    }

    @Test
    public void test_defaultMethod_andExternalConfiguration() {
        probabilities.put("timeStep1Prob", 0.3);
        probabilities.put("timeStep2Prob", -1.0);

        TimeStepModel model = loadModel("public class CLAZZ{\n"
                + "@TimeStep(prob=0.2) public void timeStep1(){}\n"
                + "@TimeStep(prob=0.8) public void timeStep2(){}\n"
                + "}\n", probabilities);

        assertProbability(model, "timeStep1", 0.3);
        assertProbability(model, "timeStep2", 0.7);
        assertNotNull(model.getTimeStepProbabilityArray(""));
    }

    @Test
    public void test_totalProbability() {
        TimeStepModel model = loadModel("public class CLAZZ{\n"
                + "@TimeStep(prob=0.4) public void timeStep1(){}\n"
                + "@TimeStep(prob=0.2) public void timeStep2(){}\n"
                + "@TimeStep(prob=0.15) public void timeStep3(){}\n"
                + "@TimeStep(prob=0.2) public void timeStep4(){}\n"
                + "@TimeStep(prob=0.05) public void timeStep5(){}\n"
                + "}\n", probabilities);

        Probability totalProbability = new Probability(0);
        for (Method method : model.getActiveTimeStepMethods("")) {
            Probability probability = model.getProbability("", method.getName());
            totalProbability = totalProbability.add(probability);
        }

        assertEquals(1.0, totalProbability.getValue(), DOUBLE_TOLERANCE_DELTA);
    }

    private static TimeStepModel loadModel(String code, Map<String, Double> probabilities) {
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
        for (Map.Entry<String, Double> entry : probabilities.entrySet()) {
            testCase.setProperty(entry.getKey(), entry.getValue());
        }
        return new TimeStepModel(clazz, new PropertyBinding(testCase));
    }

    private static void assertProbability(TimeStepModel model, String method, double value) {
        assertEquals(value, model.getProbability("", method).getValue(), DOUBLE_TOLERANCE_DELTA);
    }

    private static void assertProbability(TimeStepModel model, String group, String method, double value) {
        assertEquals(value, model.getProbability(group, method).getValue(), DOUBLE_TOLERANCE_DELTA);
    }
}
