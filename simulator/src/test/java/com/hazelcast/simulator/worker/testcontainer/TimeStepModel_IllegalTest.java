/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hazelcast.simulator.worker.testcontainer;

import com.hazelcast.simulator.common.TestCase;
import com.hazelcast.simulator.test.annotations.TimeStep;
import com.hazelcast.simulator.utils.compiler.InMemoryJavaCompiler;
import org.junit.Test;

import java.util.UUID;

import static org.junit.Assert.fail;

public class TimeStepModel_IllegalTest {

    // ====================== StartNanos ===========================
    @Test
    public void test_intendedStartTimeIllegalType() {
        assertBroken("class CLAZZ{\n"
                + "@TimeStep public void timeStep(@StartNanos double x){}\n"
                + "}\n");
    }

    // ====================== threadState ===========================

    @Test
    public void test_threadState_interface() {
        assertBroken("class CLAZZ{\n"
                + "@TimeStep public void timeStep(List a1){}\n"
                + "}\n");
    }

    @Test
    public void test_threadState_abstractClass() {
        assertBroken("class CLAZZ{\n"
                + "@TimeStep public void timeStep(AbstractList a1){}\n"
                + "}\n");
    }

    @Test
    public void test_threadState_primitive() {
        assertBroken("class CLAZZ{\n"
                + "@TimeStep public void timeStep(int a1){}\n"
                + "}\n");
    }

    @Test(expected = IllegalTestException.class)
    public void test_threadState_notPublicClass() {
        TestCase testCase = new TestCase("id");
        PropertyBinding binding = new PropertyBinding(testCase);
        new TimeStepModel(TestWithPrivateThreadState.class, binding);
    }

    public static class TestWithPrivateThreadState {
        private class BadThreadState {
        }

        @TimeStep
        public void timeStep(BadThreadState state) {
        }
    }

    @Test(expected = IllegalTestException.class)
    public void test_threadState_illegalArgumentTypeInConstructor() {
        TestCase testCase = new TestCase("id");
        PropertyBinding binding = new PropertyBinding(testCase);
        new TimeStepModel(TestWithContextWithIllegalArgumentTypeInConstructor.class, binding);
    }

    public static class TestWithContextWithIllegalArgumentTypeInConstructor {

        @SuppressWarnings("unused")
        public static class BadThreadState {

            public BadThreadState(int a) {
            }
        }

        @TimeStep
        public void timeStep(BadThreadState state) {
        }
    }

    @Test(expected = IllegalTestException.class)
    public void test_threadState_illegalArgumentCountInConstructor() {
        TestCase testCase = new TestCase("id");
        PropertyBinding binding = new PropertyBinding(testCase);
        new TimeStepModel(TestWithContextWithIllegalArgumentCountConstructor.class, binding);
    }

    public static class TestWithContextWithIllegalArgumentCountConstructor {

        @SuppressWarnings("unused")
        public static class BadThreadState {

            public BadThreadState(TestWithContextWithIllegalArgumentCountConstructor a, int b) {
            }
        }

        @TimeStep
        public void timeStep(BadThreadState state) {
        }
    }

    @Test
    public void test_threadState_conflictingTypesBetweenTimeStepMethods() {
        assertBroken("public class CLAZZ{\n"
                + "@TimeStep(prob=0.5) public void timeStep1(ArrayList c){}\n"
                + "@TimeStep(prob=0.5) public void timeStep2(LinkedList c){}\n"
                + "}\n");
    }

    @Test
    public void test_threadState_conflictingTypesBetweenTimeStepAndBeforeRun() {
        assertBroken("public class CLAZZ{\n"
                + "@BeforeRun public void beforeRun(ArrayList c){}\n"
                + "@TimeStep(prob=0.5) public void timeStep2(LinkedList c){}\n"
                + "}\n");
    }

    @Test
    public void test_threadState_conflictingTypesBetweenTimeStepAndAfterRun() {
        assertBroken("public class CLAZZ{\n"
                + "@AfterRun public void afterRun(ArrayList c){}\n"
                + "@TimeStep(prob=0.5) public void timeStep2(LinkedList c){}\n"
                + "}\n");
    }

    // ====================== arguments ===========================

    @Test
    public void test_tooManyArgumentsForTimeStep() {
        assertBroken("public class CLAZZ{\n"
                + "@TimeStep public void timeStep(ArrayList a1, ArrayList a2, ArrayList a3, ArrayList a4){}\n"
                + "}\n");
    }

    @Test
    public void test_tooManyArgumentsForAfterRunMethod() {
        assertBroken("public class CLAZZ{\n"
                + "@BeforeRun public void beforeRun(BaseThreadState a1, int a2){}\n"
                + "@TimeStep public void timeStep(){}\n"
                + "}\n");
    }

    @Test
    public void test_tooManyArgumentsForBeforeRunMethod() {
        assertBroken("public class CLAZZ{\n"
                + "@TimeStep public void timeStep(){}\n"
                + "@BeforeRun public void beforeRun(BaseThreadState a1, int a2){}\n"
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

    // ===========================================

    @Test
    public void test_singleAfterRun() {
        assertBroken("class CLAZZ{\n"
                + "@AfterRun public void afterRun(){}\n"
                + "}\n");
    }

    @Test
    public void test_singleBeforeRun() {
        assertBroken("class CLAZZ{\n"
                + "@BeforeRun public void beforeRun(){}\n"
                + "}\n");
    }

    @Test
    public void test_invalidGroupName() {
        assertBroken("class CLAZZ{\n"
                + "@TimeStep(executionGroup=\".\") public void timeStep(){}\n"
                + "}\n");
    }

    private static void assertBroken(String source) {
        String header = "import java.util.*;\n"
                + " import com.hazelcast.simulator.test.*;\n"
                + "import com.hazelcast.simulator.test.annotations.*;\n"
                + "import com.hazelcast.simulator.test.annotations.*;\n";
        source = header + source;
        String className = "CLAZZ" + UUID.randomUUID().toString().replace("-", "");
        source = source.replace("CLAZZ", className);

        Class clazz;
        try {
            clazz = InMemoryJavaCompiler.compile(className, source);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        TestCase testCase = new TestCase("foo").setProperty("class", clazz.getName());
        try {
            new TimeStepModel(clazz, new PropertyBinding(testCase));
            fail("Expected IllegalTestException in TimeStepModel constructor");
        } catch (IllegalTestException e) {
            e.printStackTrace();
        }
    }
}
