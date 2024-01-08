/*
 * Copyright (c) 2008-2024, Hazelcast, Inc. All Rights Reserved.
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
package com.hazelcast.simulator.tests.ucd;

import com.hazelcast.simulator.hz.HazelcastTest;
import com.hazelcast.simulator.test.annotations.Setup;

import java.net.URL;
import java.net.URLClassLoader;

import static org.junit.Assert.assertNotNull;

/**
 * Extendable test framework for Namespace performance testing.
 * <p>
 * Responsible for loading the UDF class with a URLClassLoader and
 * optionally configuring a namespace with the loaded class.
 * <p>
 * Provides protected access to the loaded {@link #udf} for subclasses.
 */
public class UCDTest extends HazelcastTest {

    /*
   The directory where the UDF class can be found.
   */
    public String classDir;
    /*
    Fully qualified name of the class.
     */
    public String className;
    /*
    If namespaces should be enabled and the class loaded.
    Note : it is required that the hazelcast.xml has a namespace
    pre-configured.
     */
    public boolean useNamespace;
    /*
    The id of the namespace to add the class to. Must exist in the
    hazelcast.xml.
     */
    public String namespaceId;
    /*
    The UDF class which is loaded with a URLClassLoader using the given
    classDir and className test properties.
     */
    protected Class<?> udf;

    /**
     * Preloads UDF and configures namespace if required.
     *
     * @throws ClassNotFoundException if udf not found.
     */
    @Setup
    public void setUp() throws ReflectiveOperationException {
        loadUDF();
        if (useNamespace) {
            configureNamespace();
        }
    }

    /**
     * Loads the UDF class with a URL classloader.
     *
     * @throws ClassNotFoundException if udf not found.
     */
    @SuppressWarnings("resource")
    private void loadUDF() throws ClassNotFoundException {
        assertNotNull("classDir must be set", classDir);
        assertNotNull("className must be set", className);
        URL url = getClass().getClassLoader().getResource(classDir);
        this.udf = new URLClassLoader(new URL[]{url}).loadClass(className);
    }

    /**
     * Configures the loaded udf as a namespace with the configured
     * namespacedId.
     * <p>
     * Note: this ID must be configured in the hazelcast.xml and should
     * be applied to the hazelcast data structure in the hazelcast.xml.
     */
    private void configureNamespace() {
        assertNotNull("namespaceId must be set", namespaceId);
        assertNotNull("udf must be loaded", udf);
//        NamespaceConfig nsc = new NamespaceConfig().setName(namespaceId).addClass(udf);
//        targetInstance.getConfig().getNamespacesConfig()
//                .addNamespaceConfig(nsc);
        throw new IllegalArgumentException("UCD not supported in this version of Hazelcast!");
    }
}
