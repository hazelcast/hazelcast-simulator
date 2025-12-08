package com.hazelcast.simulator.utils;

import org.junit.Test;

import javax.tools.JavaFileObject;

import java.io.File;

import static org.mockito.Mockito.mock;

public class CompilationUtilsTest {

    @Test(expected = IllegalStateException.class)
    public void testCompile_whenCompilerIsNull_thenThrowIllegalStateException() {
        JavaFileObject javaFileObject = mock(JavaFileObject.class);

        CompilationUtils.compile(javaFileObject, "className", new File(""));
    }
}
