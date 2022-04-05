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

import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

final class DynamicClassLoader extends ClassLoader {

    private static final DynamicClassLoader INSTANCE;

    static {
        INSTANCE = AccessController.doPrivileged(new PrivilegedAction<DynamicClassLoader>() {

            public DynamicClassLoader run() {
                return new DynamicClassLoader(ClassLoader.getSystemClassLoader());
            }
        });
    }

    private final Map<String, CompiledCode> customCompiledCode = new ConcurrentHashMap<String, CompiledCode>();

    private DynamicClassLoader(ClassLoader parent) {
        super(parent);
    }

    public static DynamicClassLoader getInstance() {
        return INSTANCE;
    }

    public void setCode(CompiledCode compiledCode) {
        customCompiledCode.put(compiledCode.getName(), compiledCode);
    }

    @Override
    protected Class<?> findClass(String name) throws ClassNotFoundException {
        CompiledCode compiledCode = customCompiledCode.get(name);
        if (compiledCode == null) {
            return super.findClass(name);
        }
        byte[] byteCode = compiledCode.getByteCode();
        return defineClass(name, byteCode, 0, byteCode.length);
    }
}
