package com.hazelcast.simulator.worker;

import com.hazelcast.simulator.test.IllegalTestException;
import com.hazelcast.simulator.test.PropertyBinding;
import com.hazelcast.simulator.test.TestCase;
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
        assertNull(model.getTimeStepProbabilityArray());
    }

    private void assertProbability(TimeStepModel model, String method, double value) {
        assertEquals(value, model.getProbability(method).getValue(), 0.001);
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
        assertNull(model.getTimeStepProbabilityArray());
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
        assertNull(model.getTimeStepProbabilityArray());
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
        assertNotNull(model.getTimeStepProbabilityArray());
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
        assertNotNull(model.getTimeStepProbabilityArray());
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
        assertNotNull(model.getTimeStepProbabilityArray());
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

        TestCase testCase = new TestCase("foo").setProperty("class", clazz.getName());
        for (Map.Entry<String, Double> entry : probs.entrySet()) {
            testCase.setProperty(entry.getKey(), entry.getValue());
        }
        return new TimeStepModel(clazz, new PropertyBinding(testCase));
    }
}
