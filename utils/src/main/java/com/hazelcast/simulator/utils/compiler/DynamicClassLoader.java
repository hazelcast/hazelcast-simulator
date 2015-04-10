package com.hazelcast.simulator.utils.compiler;

import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

class DynamicClassLoader extends ClassLoader {

    private final static DynamicClassLoader INSTANCE;

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
