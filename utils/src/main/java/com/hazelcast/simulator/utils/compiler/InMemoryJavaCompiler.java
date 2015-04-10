package com.hazelcast.simulator.utils.compiler;

import javax.tools.JavaCompiler;
import javax.tools.JavaFileObject;
import javax.tools.StandardJavaFileManager;
import javax.tools.ToolProvider;
import java.util.Collections;

/**
 * In-memory Java source code compiler.
 *
 * @see <a href="https://github.com/trung/InMemoryJavaCompiler">InMemoryJavaCompiler GitHub Project</a>
 */
public final class InMemoryJavaCompiler {

    static final JavaCompiler COMPILER = ToolProvider.getSystemJavaCompiler();

    private InMemoryJavaCompiler() {
    }

    public static Class<?> compile(String className, String sourceCodeInText) throws Exception {
        SourceCode sourceCode = new SourceCode(className, sourceCodeInText);
        CompiledCode compiledCode = new CompiledCode(className);
        DynamicClassLoader classLoader = DynamicClassLoader.getInstance();
        Iterable<? extends JavaFileObject> compilationUnits = Collections.singletonList(sourceCode);

        StandardJavaFileManager standardJavaFileManager = COMPILER.getStandardFileManager(null, null, null);
        ExtendedJavaFileManager fileManager = new ExtendedJavaFileManager(standardJavaFileManager, compiledCode, classLoader);

        JavaCompiler.CompilationTask task = COMPILER.getTask(null, fileManager, null, null, null, compilationUnits);
        task.call();

        return classLoader.loadClass(className);
    }
}
