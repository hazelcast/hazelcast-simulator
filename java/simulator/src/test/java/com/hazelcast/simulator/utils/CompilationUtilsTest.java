package com.hazelcast.simulator.utils;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import javax.tools.JavaFileObject;
import java.io.File;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.Assert.assertEquals;

public class CompilationUtilsTest {

    @Rule
    public TemporaryFolder tempDir = new TemporaryFolder();

    @Test
    public void testKind() {
        assertEquals(JavaFileObject.Kind.SOURCE, CompilationUtils.kindOf(Path.of("upload/Something.java")));
        assertEquals(JavaFileObject.Kind.CLASS, CompilationUtils.kindOf(Path.of("upload/Something.class")));
    }

    @Test
    public void testDynamicCompilationWithoutPackage()
            throws Exception {
        testDynamicCompilation(null);
    }

    @Test
    public void testDynamicCompilationWithPackage()
            throws Exception {
        testDynamicCompilation("com.hz.test");
    }

    private void testDynamicCompilation(String pkg)
            throws Exception {
        Path input = tempDir.getRoot().toPath().resolve("Example.java");
        String pkgStatement = pkg == null ? "" : "package " + pkg + ";";
        Files.writeString(input, String.format("""
                %s
                public class Example {
                    public int getValue() {
                        return 42;
                    }
                }
                """, pkgStatement));

        File output = tempDir.newFolder();
        String pkgPrefix = pkg == null ? "" : pkg + ".";
        Class<?> cls = CompilationUtils.compile(input, pkgPrefix + "Example", output);
        Method m = cls.getMethod("getValue");
        assertEquals(42, m.invoke(cls.getConstructor().newInstance()));
    }
}
