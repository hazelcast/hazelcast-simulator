package com.hazelcast.simulator.worker;

import com.hazelcast.simulator.test.IllegalTestException;
import com.hazelcast.simulator.test.PropertyBinding;
import com.hazelcast.simulator.test.TestCase;
import com.hazelcast.simulator.test.annotations.TimeStep;
import com.hazelcast.simulator.utils.compiler.InMemoryJavaCompiler;
import org.junit.Test;

import java.util.UUID;

import static org.junit.Assert.fail;

public class TimeStepModel_IllegalTest {


    // ====================== threadContext ===========================

    @Test
    public void test_threadContext_interface() {
        assertBroken("class CLAZZ{\n"
                + "@TimeStep public void timeStep(List a1){}\n"
                + "}\n");
    }

    @Test
    public void test_threadContext_abstractClass() {
        assertBroken("class CLAZZ{\n"
                + "@TimeStep public void timeStep(AbstractList a1){}\n"
                + "}\n");
    }

    @Test
    public void test_threadContext_primitive() {
        assertBroken("class CLAZZ{\n"
                + "@TimeStep public void timeStep(int a1){}\n"
                + "}\n");
    }

    @Test(expected = IllegalTestException.class)
    public void test_threadContext_notPublicClass() {
        TestCase testCase = new TestCase("id");
        PropertyBinding binding = new PropertyBinding(testCase);
        new TimeStepModel(TestWithPrivateContext.class,binding);
    }

    public static class TestWithPrivateContext {
        private class BadContext {
        }

        @TimeStep public void timestep(BadContext badContext){}
    }

    @Test(expected = IllegalTestException.class)
    public void test_threadContext_illegalArgumentTypeInConstructor() {
        TestCase testCase = new TestCase("id");
        PropertyBinding binding = new PropertyBinding(testCase);
        new TimeStepModel(TestWithContextWithIllegalArgumentTypeInConstructor.class,binding);
    }

    public static class TestWithContextWithIllegalArgumentTypeInConstructor {
        public static class BadContext {
            public BadContext(int a){
            }
        }

        @TimeStep public void timestep(BadContext badContext){}
    }

    @Test(expected = IllegalTestException.class)
    public void test_threadContext_illegalArgumentCountInConstructor() {
        TestCase testCase = new TestCase("id");
        PropertyBinding binding = new PropertyBinding(testCase);
        new TimeStepModel(TestWithContextWithIllegalArgumentCountConstructor.class,binding);
    }

    public static class TestWithContextWithIllegalArgumentCountConstructor {
        public static class BadContext {
            public BadContext(TestWithContextWithIllegalArgumentCountConstructor a, int b){
            }
        }

        @TimeStep public void timestep(BadContext badContext){}
    }

    @Test
    public void test_threadContext_conflictingTypesBetweenTimeStepMethods() {
        assertBroken("public class CLAZZ{\n"
                + "@TimeStep(prob=0.5) public void timeStep1(ArrayList c){}\n"
                + "@TimeStep(prob=0.5) public void timeStep2(LinkedList c){}\n"
                + "}\n");
    }

    @Test
    public void test_threadContext_conflictingTypesBetweenTimeStepAndBeforeRun() {
        assertBroken("public class CLAZZ{\n"
                + "@BeforeRun public void beforeRun(ArrayList c){}\n"
                + "@TimeStep(prob=0.5) public void timeStep2(LinkedList c){}\n"
                + "}\n");
    }

    @Test
    public void test_threadContext_conflictingTypesBetweenTimeStepAndAfterRun() {
        assertBroken("public class CLAZZ{\n"
                + "@AfterRun public void afterRun(ArrayList c){}\n"
                + "@TimeStep(prob=0.5) public void timeStep2(LinkedList c){}\n"
                + "}\n");
    }

    // ====================== arguments ===========================

    @Test
    public void test_tooManyArgumentsForTimeStep() {
        assertBroken("public class CLAZZ{\n"
                + "@TimeStep public void timeStep(ArrayList a1, ArrayList a2, ArrayList a3){}\n"
                + "}\n");
    }

    @Test
    public void test_tooManyArgumentsForAfterRunMethod() {
        assertBroken("public class CLAZZ{\n"
                + "@BeforeRun public void beforeRun(BaseThreadContext a1, int a2){}\n"
                + "@TimeStep public void timeStep(){}\n"
                + "}\n");
    }

    @Test
    public void test_tooManyArgumentsForBeforeRunMethod() {
        assertBroken("public class CLAZZ{\n"
                + "@TimeStep public void timeStep(){}\n"
                + "@BeforeRun public void beforeRun(BaseThreadContext a1, int a2){}\n"
                + "}\n");
    }

    // ====================== access modifiers ===========================

    @Test
    public void test_accessModifier_privateMethod() {
        assertBroken("public class CLAZZ{\n"
                + "@TimeStep private void timeStep(){}\n"
                + "}\n");
    }

    @Test
    public void test_accessModifier_protectedMethod() {
        assertBroken("public class CLAZZ{\n"
                + "@TimeStep protected void timeStep(){}\n"
                + "}\n");
    }

    @Test
    public void test_accessModifier_packageFriendlyMethod() {
        assertBroken("public class CLAZZ{\n"
                + "@TimeStep void timeStep(){}\n"
                + "}\n");
    }

    @Test
    public void test_accessModifier_staticMethod() {
        assertBroken("public class CLAZZ{\n"
                + "@TimeStep public static void timeStep(){}\n"
                + "}\n");
    }

    @Test
    public void test_duplicateMethod() {
        assertBroken("public class CLAZZ{\n"
                + "@TimeStep(prob=0.5) public void timeStep(){}\n"
                + "@TimeStep(prob=0.5) public void timeStep(ArrayList context){}\n"
                + "}\n");
    }

    // ====================== access modifiers ===========================

    @Test
    public void test_probability_smallerThanZero() {
        assertBroken("public class CLAZZ{\n"
                + "@TimeStep(prob=-5) public void timeStep(){}\n"
                + "}\n");
    }

    @Test
    public void test_probability_largerThanOne() {
        assertBroken("public class CLAZZ{\n"
                + "@TimeStep(prob=1.1) public void timeStep(){}\n"
                + "}\n");
    }

    @Test
    public void test_probability_singleMethodSumNotOne() {
        assertBroken("public class CLAZZ{\n"
                + "@TimeStep(prob=0.1) public void timeStep(){}\n"
                + "}\n");
    }

    @Test
    public void test_probability_multipleMethodsSumNotOne() {
        assertBroken("public class CLAZZ{\n"
                + "@TimeStep(prob=0.1) public void timeStep1(){}\n"
                + "@TimeStep(prob=0.1) public void timeStep2(){}\n"
                + "}\n");
    }

    @Test
    public void test_probability_multipleMethodsExceedOne() {
        assertBroken("public class CLAZZ{\n"
                + "@TimeStep(prob=1) public void timeStep1(){}\n"
                + "@TimeStep(prob=1) public void timeStep2(){}\n"
                + "}\n");
    }


    public void assertBroken(String s) {
        String header = "import java.util.*;\n"
                +" import com.hazelcast.simulator.test.*;\n"
                + "import com.hazelcast.simulator.test.annotations.*;\n"
                + "import com.hazelcast.simulator.test.annotations.*;\n";
        s = header + s;
        String className = "CLAZZ" + UUID.randomUUID().toString().replace("-", "");
        s = s.replace("CLAZZ", className);

        Class clazz;
        try {
            clazz = InMemoryJavaCompiler.compile(className, s);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        TestCase testCase = new TestCase("foo").setProperty("class", clazz.getName());
        try {
            new TimeStepModel(clazz, new PropertyBinding(testCase));
            fail();
        } catch (IllegalTestException e) {
            e.printStackTrace();
        }
    }
}
