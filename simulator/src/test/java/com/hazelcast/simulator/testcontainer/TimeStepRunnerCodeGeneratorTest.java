package com.hazelcast.simulator.testcontainer;

import com.hazelcast.simulator.testcontainer.TimeStepRunnerCodeGenerator;
import org.junit.Test;

import javax.tools.JavaFileObject;

import static org.mockito.Mockito.mock;

public class TimeStepRunnerCodeGeneratorTest {

    private TimeStepRunnerCodeGenerator codeGenerator = new TimeStepRunnerCodeGenerator();

    @Test(expected = IllegalStateException.class)
    public void testCompile_whenCompilerIsNull_thenThrowIllegalStateException() {
        JavaFileObject javaFileObject = mock(JavaFileObject.class);

        codeGenerator.compile(null, javaFileObject, "className");
    }
}
