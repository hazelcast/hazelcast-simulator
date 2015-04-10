package com.hazelcast.simulator.utils.compiler;

import javax.tools.FileObject;
import javax.tools.ForwardingJavaFileManager;
import javax.tools.JavaFileManager;
import javax.tools.JavaFileObject;
import javax.tools.JavaFileObject.Kind;
import java.io.IOException;

class ExtendedJavaFileManager extends ForwardingJavaFileManager<JavaFileManager> {

    private final CompiledCode compiledCode;
    private final DynamicClassLoader dynamicClassLoader;

    /**
     * Creates a new instance of ForwardingJavaFileManager.
     *
     * @param fileManager        delegate to this file manager
     * @param dynamicClassLoader dynamic class loader
     */
    protected ExtendedJavaFileManager(JavaFileManager fileManager, CompiledCode compiledCode,
                                      DynamicClassLoader dynamicClassLoader) {
        super(fileManager);
        this.compiledCode = compiledCode;
        this.dynamicClassLoader = dynamicClassLoader;

        dynamicClassLoader.setCode(compiledCode);
    }

    @Override
    public JavaFileObject getJavaFileForOutput(Location location, String className, Kind kind, FileObject sibling)
            throws IOException {
        return compiledCode;
    }

    @Override
    public ClassLoader getClassLoader(Location location) {
        return dynamicClassLoader;
    }
}
