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

import javax.tools.FileObject;
import javax.tools.ForwardingJavaFileManager;
import javax.tools.JavaFileManager;
import javax.tools.JavaFileObject;
import javax.tools.JavaFileObject.Kind;

class ExtendedJavaFileManager extends ForwardingJavaFileManager<JavaFileManager> {

    private final CompiledCode compiledCode;
    private final DynamicClassLoader dynamicClassLoader;

    /**
     * Creates a new instance of ForwardingJavaFileManager.
     *
     * @param fileManager        delegate to this file manager
     * @param dynamicClassLoader dynamic class loader
     */
    ExtendedJavaFileManager(JavaFileManager fileManager, CompiledCode compiledCode, DynamicClassLoader dynamicClassLoader) {
        super(fileManager);
        this.compiledCode = compiledCode;
        this.dynamicClassLoader = dynamicClassLoader;

        dynamicClassLoader.setCode(compiledCode);
    }

    @Override
    public JavaFileObject getJavaFileForOutput(Location location, String className, Kind kind, FileObject sibling) {
        return compiledCode;
    }

    @Override
    public ClassLoader getClassLoader(Location location) {
        return dynamicClassLoader;
    }
}
