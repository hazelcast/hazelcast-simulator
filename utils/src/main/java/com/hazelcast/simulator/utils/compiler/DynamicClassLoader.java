package com.hazelcast.simulator.utils.compiler;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

class DynamicClassLoader extends ClassLoader {

    private final Map<String, CompiledCode> customCompiledCode = new ConcurrentHashMap<String, CompiledCode>();

    public DynamicClassLoader(ClassLoader parent) {
        super(parent);
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
