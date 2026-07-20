package com.hazelcast.simulator.worker.testcontainer;

import com.hazelcast.simulator.common.TestCase;
import com.hazelcast.simulator.test.annotations.TimeStep;
import com.hazelcast.simulator.worker.metronome.EmptyMetronome;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.tools.JavaFileObject;
import javax.tools.ToolProvider;
import java.io.IOException;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.mockito.Mockito.mock;

public class TimeStepLoopCodeGeneratorTest {

    private static final String RANDOM_SELECTOR = "probs[random.nextInt(probs.length)]";
    private static final String SEQUENTIAL_SELECTOR = "iteration % 2";
    private static final String SEQUENTIAL_SELECTOR_3 = "iteration % 3";
    private static final Logger log = LoggerFactory.getLogger(TimeStepLoopCodeGeneratorTest.class);

    private final TimeStepLoopCodeGenerator codeGenerator = new TimeStepLoopCodeGenerator();

    @Test(expected = IllegalStateException.class)
    public void testCompile_whenCompilerIsNull_thenThrowIllegalStateException() {
        JavaFileObject javaFileObject = mock(JavaFileObject.class);

        codeGenerator.compile(null, javaFileObject, "className");
    }

    public static class TestClass1 {
        @TimeStep(prob = 0)
        public void method3() {
        }

        @TimeStep(prob = 0)
        public void method1() {
        }

        @TimeStep(prob = 0)
        public void method2() {
        }
    }

    @Test
    public void testGenerateSingleMethodRandom() throws IOException {
        testGenerateSingleMethod(false);
    }

    @Test
    public void testGenerateSingleMethodSequential() throws IOException {
        testGenerateSingleMethod(true);
    }

    private void testGenerateSingleMethod(boolean sequential) throws IOException {
        var generatedCode = generateTestClass(Map.of("method1Prob", "1"), sequential);

        assertThat(generatedCode)
                .as("should not select methods")
                .doesNotContain(RANDOM_SELECTOR).doesNotContain(SEQUENTIAL_SELECTOR)
                .as("should invoke configured method")
                .contains("method1( )").doesNotContain("method2( )");
    }

    @Test
    public void testGenerateRandom() throws IOException {
        var generatedCode = generateTestClass(Map.of("method1Prob", "0.5", "method2Prob", "0.5"), false);

        assertThat(generatedCode)
                .as("should invoke methods in sequence")
                .contains(RANDOM_SELECTOR)
                .as("should invoke all methods")
                .contains("method1( )", "method2( )");
    }

    @Test
    public void testGenerateSequential() throws IOException {
        var generatedCode = generateTestClass(Map.of("method1Prob", "0.5", "method2Prob", "0.5"), true);

        assertThat(generatedCode)
                .as("should invoke methods in sequence")
                .contains(SEQUENTIAL_SELECTOR)
                .as("should sort methods alphabetically")
                .containsSubsequence("method1( )", "method2( )");
    }

    @Test
    public void testGenerateSequential_3methods() throws IOException {
        var generatedCode = generateTestClass(Map.of("method1Prob", "0.5", "method2Prob", "0.25", "method3Prob", "0.25"), true);

        assertThat(generatedCode)
                .as("should invoke methods in sequence")
                .contains(SEQUENTIAL_SELECTOR_3)
                .as("should sort methods alphabetically")
                .containsSubsequence("method1( )", "method2( )", "method3( )");
    }

    private CharSequence generateTestClass(Map<String, String> properties, boolean sequential) throws IOException {
        // minimal scaffolding required for code generation
        var testCase = new TestCase("someTest", properties);
        var model = new TimeStepModel(TestClass1.class, new PropertyBinding(testCase));

        var generatedCode = codeGenerator.createJavaFileObject(model.getTestClass().getSimpleName() + "Loop", "", EmptyMetronome.class,
                model, null, 0, 0, false, sequential);
        log.info("generated code: {}", generatedCode.getCharContent(false));

        assertThatNoException()
                .as("generated code should be valid")
                .isThrownBy(() -> codeGenerator.compile(ToolProvider.getSystemJavaCompiler(), generatedCode, model.getTestClass().getName()));

        return generatedCode.getCharContent(false);
    }
}
