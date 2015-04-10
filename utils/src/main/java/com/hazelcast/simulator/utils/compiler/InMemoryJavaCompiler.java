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

    static final DynamicClassLoader CLASS_LOADER = new DynamicClassLoader(ClassLoader.getSystemClassLoader());
    static final JavaCompiler JAVAC = ToolProvider.getSystemJavaCompiler();

    private InMemoryJavaCompiler() {
    }

    public static Class<?> compile(String className, String sourceCodeInText) throws Exception {
        CompiledCode compiledCode = new CompiledCode(className);
        SourceCode sourceCode = new SourceCode(className, sourceCodeInText);
        Iterable<? extends JavaFileObject> compilationUnits = Collections.singletonList(sourceCode);

        StandardJavaFileManager standardJavaFileManager = JAVAC.getStandardFileManager(null, null, null);
        ExtendedJavaFileManager fileManager = new ExtendedJavaFileManager(standardJavaFileManager, compiledCode, CLASS_LOADER);

        JavaCompiler.CompilationTask task = JAVAC.getTask(null, fileManager, null, null, null, compilationUnits);
        task.call();

        return CLASS_LOADER.loadClass(className);
    }
}
