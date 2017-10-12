/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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
package com.hazelcast.simulator.utils;

import com.hazelcast.simulator.common.TestCase;
import org.junit.Test;

import static com.hazelcast.simulator.utils.PropertyBindingSupport.bind;
import static com.hazelcast.simulator.utils.PropertyBindingSupport.bind0;
import static com.hazelcast.simulator.utils.ReflectionUtils.invokePrivateConstructor;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class PropertyBindingSupport_Test {

    private final TestCase testCase = new TestCase(getClass().getName());
    private final SomeObject someObject = new SomeObject();

    @Test
    public void testConstructor() throws Exception {
        invokePrivateConstructor(PropertyBindingSupport.class);
    }

    @Test
    public void testBind_testcaseIsNull() {
        bind(someObject, null, "ignored");
    }

    @Test
    public void testBind_propertyNotDefined() {
        testCase.setProperty("class", "willBeIgnored");
        testCase.setProperty("notExist", "isOptional");

        bind(someObject, testCase, "propertyNotDefined");
    }

    @Test
    public void testBind_propertyNotFound() {
        testCase.setProperty("class", "willBeIgnored");
        testCase.setProperty("notExist", "isOptional");

        bind(someObject, testCase, "notExist");
    }

    @Test
    public void testBind() {
        testCase.setProperty("class", "willBeIgnored");
        testCase.setProperty("stringField", "foo");

        bind(someObject, testCase, "stringField");
        assertEquals("foo", someObject.stringField);
    }

    @Test
    public void bind_withPath() {
        bind0(someObject, "otherObject.stringField", "newValue");
        assertEquals("newValue", someObject.otherObject.stringField);
    }

    @Test(expected = BindException.class)
    public void bind_withPathAndNullValue() {
        bind0(someObject, "nullOtherObject.stringField", "newValue");
    }

    @Test
    public void bind_withPath_missingProperty() {
        assertFalse(bind0(someObject, "notExist.stringField", "newValue"));
    }

    @Test
    public void bind_unknownField() {
        boolean result = bind0(someObject, "notExist", "null");

        assertFalse(result);
    }

    @Test(expected = BindException.class)
    public void bind_protectedField() {
        bind0(someObject, "protectedField", "newValue");
    }

    @Test(expected = BindException.class)
    public void bind_packagePrivateField() {
        bind0(someObject, "packagePrivateField", "newValue");
    }

    @Test(expected = BindException.class)
    public void bind_privateField() {
        bind0(someObject, "privateField", "newValue");
    }

    @Test(expected = BindException.class)
    public void bind_staticField() {
        OtherObject otherObject = new OtherObject();
        bind0(otherObject, "staticField", "newValue");
    }

    @Test(expected = BindException.class)
    public void bind_finalField() {
        bind0(someObject, "finalField", "newValue");
    }

    @Test(expected = BindException.class)
    public void bind_illegalType() {
        bind0(someObject, "unrecognizedFieldType", "newValue");
    }

    @Test(expected = BindException.class)
    public void bind_propertyTypeMismatch() {
        bind0(someObject, "intField", "foobar");
    }

    @SuppressWarnings({"unused", "WeakerAccess"})
    private class SomeObject {

        public int intField = 5;
        public final int finalField = 5;

        public String stringField;

        // we can't bind to this field because its type is not known
        public Comparable unrecognizedFieldType;

        public OtherObject otherObject = new OtherObject();
        public OtherObject nullOtherObject;

        protected String protectedField;
        String packagePrivateField;
        private String privateField;
    }

    @SuppressWarnings({"unused", "WeakerAccess"})
    private static class OtherObject {

        public static Object staticField;

        public String stringField;
    }
}
