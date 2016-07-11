package com.hazelcast.simulator.worker;

import com.hazelcast.simulator.test.DependencyInjector;
import com.hazelcast.simulator.test.IllegalTestException;
import com.hazelcast.simulator.test.TestCase;
import com.hazelcast.simulator.test.TestContext;
import com.hazelcast.simulator.utils.compiler.InMemoryJavaCompiler;
import org.junit.Test;

import java.util.UUID;

import static com.hazelcast.simulator.utils.ClassUtils.getClassName;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

public class TimeStepModel_IllegalTest {


    // ====================== threadContext ===========================

    private class PrivateContext {
    }


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

    @Test
    public void test_threadContext_nonePublicClass() {
        assertBroken("class CLAZZ{\n"
                + "@TimeStep public void timeStep(" + PrivateContext.class.getName() + " a1){}\n"
                + "}\n");
    }

    @Test
    public void test_threadContext_conflictingTypes() {
        assertBroken("class CLAZZ{\n"
                + "@TimeStep(prob=0.5) public void timeStep1(ArrayList c){}\n"
                + "@TimeStep(prob=0.5) public void timeStep2(LinkedList c){}\n"
                + "}\n");
    }

    public static class NonPublicConstructor {
        private NonPublicConstructor() {
        }
    }

    @Test
    public void test_threadContext_nonePublicConstructor() {
        assertBroken("class CLAZZ{\n"
                + "@TimeStep public void timeStep1(" + getClassName(NonPublicConstructor.class) + " c){}\n"
                + "}\n");
    }


    // ====================== arguments ===========================


    @Test
    public void test_tooManyArguments() {
        assertBroken("class CLAZZ{\n"
                + "@TimeStep public void timeStep(ArrayList a1, ArrayList a2, ArrayList a3){}\n"
                + "}\n");
    }

    @Test
    public void test_accessModifier_privateMethod() {
        assertBroken("class CLAZZ{\n"
                + "@TimeStep private void timeStep(){}\n"
                + "}\n");
    }

    // ====================== access modifiers ===========================

    @Test
    public void test_accessModifier_protectedMethod() {
        assertBroken("class CLAZZ{\n"
                + "@TimeStep protected void timeStep(){}\n"
                + "}\n");
    }

    @Test
    public void test_accessModifier_packageFriendlyMethod() {
        assertBroken("class CLAZZ{\n"
                + "@TimeStep void timeStep(){}\n"
                + "}\n");
    }

    @Test
    public void test_accessModifier_staticMethod() {
        assertBroken("class CLAZZ{\n"
                + "@TimeStep public static void timeStep(){}\n"
                + "}\n");
    }

    @Test
    public void test_duplicateMethod() {
        assertBroken("class CLAZZ{\n"
                + "@TimeStep(prob=0.5) public void timeStep(){}\n"
                + "@TimeStep(prob=0.5) public void timeStep(ArrayList context){}\n"
                + "}\n");
    }

    // ====================== access modifiers ===========================

    @Test
    public void test_probability_smallerThanZero() {
        assertBroken("class CLAZZ{\n"
                + "@TimeStep(prob=-1) public void timeStep(){}\n"
                + "}\n");
    }

    @Test
    public void test_probability_largerThanOne() {
        assertBroken("class CLAZZ{\n"
                + "@TimeStep(prob=1.1) public void timeStep(){}\n"
                + "}\n");
    }

    @Test
    public void test_probability_singleMethodSumNotOne() {
        assertBroken("class CLAZZ{\n"
                + "@TimeStep(prob=0.1) public void timeStep(){}\n"
                + "}\n");
    }

    @Test
    public void test_probability_multipleMethodsSumNotOne() {
        assertBroken("class CLAZZ{\n"
                + "@TimeStep(prob=0.1) public void timeStep1(){}\n"
                + "@TimeStep(prob=0.1) public void timeStep2(){}\n"
                + "}\n");
    }

    @Test
    public void test_probability_multipleMethodsExceedOne() {
        assertBroken("class CLAZZ{\n"
                + "@TimeStep(prob=1) public void timeStep1(){}\n"
                + "@TimeStep(prob=1) public void timeStep2(){}\n"
                + "}\n");
    }

    public void assertBroken(String s) {
        String header = "import java.util.*;\n"
                + "import com.hazelcast.simulator.test.annotations.*;\n"
                + "import com.hazelcast.simulator.test.annotations.*;\n";
        s = header + s;
        String className = "CLAZZ" + UUID.randomUUID().toString().replace("-","");
        s = s.replace("CLAZZ", className);

        Class clazz;
        try {
            clazz = InMemoryJavaCompiler.compile(className, s);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        TestContext context = mock(TestContext.class);
        TestCase testCase = new TestCase("foo").setProperty("class", clazz.getName());
        try {
            new TimeStepModel(clazz, new DependencyInjector(context, testCase));
            fail();
        } catch (IllegalTestException e) {
            e.printStackTrace();
        }
    }
}
