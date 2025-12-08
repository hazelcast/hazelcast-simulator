package com.hazelcast.simulator.utils;

import com.hazelcast.simulator.worker.testcontainer.IllegalTestException;

import javax.tools.Diagnostic;
import javax.tools.DiagnosticCollector;
import javax.tools.JavaCompiler;
import javax.tools.JavaFileObject;
import javax.tools.ToolProvider;
import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.security.PrivilegedAction;

import static java.security.AccessController.doPrivileged;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;

public class CompilationUtils {

    private static final JavaCompiler COMPILER = ToolProvider.getSystemJavaCompiler();

    private CompilationUtils() {
    }

    public static Class<?> compile(JavaFileObject file, String className, File targetDirectory) {
        if (COMPILER == null) {
            throw new IllegalStateException("Could not get Java compiler in TimeStepLoopCodeGenerator."
                    + " You need to use a JDK to run Simulator! Version found: " + System.getProperty("java.version"));
        }

        DiagnosticCollector<JavaFileObject> diagnostics = new DiagnosticCollector<>();
        JavaCompiler.CompilationTask task = COMPILER.getTask(null, null, diagnostics,
                asList("-d", targetDirectory.getAbsolutePath()), null, singletonList(file));

        boolean success = task.call();
        if (!success) {
            StringBuilder sb = new StringBuilder();
            for (Diagnostic<?> diagnostic : diagnostics.getDiagnostics()) {
                sb.append("Error on line ").append(diagnostic.getLineNumber()).append(" in ").append(diagnostic).append('\n');
            }
            throw new IllegalTestException(sb.toString());
        }

        return (Class<?>) doPrivileged((PrivilegedAction) () -> {
            try {
                URLClassLoader classLoader = new URLClassLoader(new URL[]{targetDirectory.toURI().toURL()});
                return (Class) classLoader.loadClass(className);
            } catch (ClassNotFoundException | MalformedURLException e) {
                throw new IllegalTestException(e.getMessage(), e);
            }
        });
    }
}
