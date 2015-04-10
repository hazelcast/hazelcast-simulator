package com.hazelcast.simulator.utils.compiler;

import javax.tools.SimpleJavaFileObject;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;

class CompiledCode extends SimpleJavaFileObject {

    private final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();

    public CompiledCode(String className) throws Exception {
        super(new URI(className), Kind.CLASS);
    }

    @Override
    public OutputStream openOutputStream() throws IOException {
        return byteArrayOutputStream;
    }

    public byte[] getByteCode() {
        return byteArrayOutputStream.toByteArray();
    }
}
