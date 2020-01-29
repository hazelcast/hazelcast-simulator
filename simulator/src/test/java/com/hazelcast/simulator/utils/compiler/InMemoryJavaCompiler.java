/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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
        System.out.println("InMemoryJavaCompiler:"+className);
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
