package com.hazelcast.simulator.utils.compiler;

import org.junit.Test;

import static com.hazelcast.simulator.utils.ReflectionUtils.invokePrivateConstructor;
import static com.hazelcast.simulator.utils.compiler.InMemoryJavaCompiler.compile;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class InMemoryJavaCompilerTest {

    @Test
    public void testConstructor() throws Exception {
        invokePrivateConstructor(InMemoryJavaCompiler.class);
    }

    @Test
    public void testCompile() throws Exception {
        String source = "package com.hazelcast.simulator.utils.compiler;\n\n"
                + "public class HelloClass {\n\n"
                + "    public String hello() {\n"
                + "        return \"hello\";\n"
                + "    }"
                + "}";

        Class<?> helloClass = compile("com.hazelcast.simulator.utils.compiler.HelloClass", source);
        assertNotNull(helloClass);
        assertEquals(1, helloClass.getDeclaredMethods().length);
    }

    @Test(expected = ClassNotFoundException.class)
    public void testDynamicClassLoader() throws Exception {
        DynamicClassLoader.getInstance().findClass("NotCompiledClass");
    }
}
