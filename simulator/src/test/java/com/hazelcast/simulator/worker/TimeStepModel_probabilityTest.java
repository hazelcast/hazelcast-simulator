package com.hazelcast.simulator.worker;

import com.hazelcast.simulator.test.PropertyBinding;
import com.hazelcast.simulator.test.TestCase;
import com.hazelcast.simulator.utils.compiler.InMemoryJavaCompiler;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.junit.Assert.assertNull;

public class TimeStepModel_probabilityTest {

    @Test
    public void test_probability_singleMethod() {
        HashMap<String, Double> probs = new HashMap<String, Double>();
        TimeStepModel model = loadModel("class CLAZZ{\n"
                + "@TimeStep public void timeStep1(){}\n"
                + "}\n", probs);

        assertNull(model.getTimeStepProbabilityArray());
    }

    @Test
    public void test_probability_multipleMethodsExceedOne() {
        HashMap<String, Double> probs = new HashMap<String, Double>();

        TimeStepModel model = loadModel("class CLAZZ{\n"
                + "@TimeStep(prob=0.5) public void timeStep1(){}\n"
                + "@TimeStep(prob=0.5) public void timeStep2(){}\n"
                + "}\n", probs);

        assertNull(model.getTimeStepProbabilityArray());
    }

    @Test
    public void test_singleActiveMethod() {
        HashMap<String, Double> probs = new HashMap<String, Double>();
        probs.put("timeStep1Prob", 1d);
        probs.put("timeStep2Prob", 0d);

        TimeStepModel model = loadModel("class CLAZZ{\n"
                + "@TimeStep(prob=0.5) public void timeStep1(){}\n"
                + "@TimeStep(prob=0.5) public void timeStep2(){}\n"
                + "}\n", probs);

        assertNull(model.getTimeStepProbabilityArray());
    }

    public TimeStepModel loadModel(String code, Map<String, Double> probs) {
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
            testCase.setProperty(entry.getKey(),entry.getValue());
        }
        return new TimeStepModel(clazz, new PropertyBinding( testCase));
    }
}
