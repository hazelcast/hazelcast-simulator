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
package com.hazelcast.simulator.utils;

import org.junit.Test;

import java.util.ArrayList;

import static com.hazelcast.simulator.utils.PropertyBindingSupport.bind0;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class PropertyBindingSupport_bind0_NonNumericalTest {

    private final TestObject testObject = new TestObject();

    @Test
    public void bind_boolean() {
        bind0(testObject, "booleanField", "true");
        assertEquals(true, testObject.booleanField);

        bind0(testObject, "booleanField", "false");
        assertEquals(false, testObject.booleanField);
    }

    @Test(expected = BindException.class)
    public void bind_boolean_invalid() {
        bind0(testObject, "booleanField", "invalid");
    }

    @Test
    public void bind_Boolean() {
        bind0(testObject, "booleanObjectField", "null");
        assertNull(testObject.booleanObjectField);

        bind0(testObject, "booleanObjectField", "true");
        assertEquals(Boolean.TRUE, testObject.booleanObjectField);

        bind0(testObject, "booleanObjectField", "false");
        assertEquals(Boolean.FALSE, testObject.booleanObjectField);
    }

    @Test(expected = BindException.class)
    public void bind_Boolean_invalid() {
        bind0(testObject, "booleanObjectField", "invalid");
    }

    @Test
    public void bind_class() {
        bind0(testObject, "clazz", "null");
        assertNull(testObject.clazz);

        bind0(testObject, "clazz", ArrayList.class.getName());
        assertEquals(ArrayList.class.getName(), testObject.clazz.getName());
    }

    @Test(expected = BindException.class)
    public void bind_class_notFound() {
        bind0(testObject, "clazz", "com.hazelnuts.simulator.utils.NotFound");
    }

    @Test
    public void bind_string() {
        bind0(testObject, "stringField", "null");
        assertNull(testObject.stringField);

        bind0(testObject, "stringField", "foo");
        assertEquals("foo", testObject.stringField);
    }

    @Test
    public void bind_enum_nullValue() {
        bind0(testObject, "enumField", "null");
        assertNull(testObject.enumField);
    }

    @Test
    public void bind_enum() {
        bind0(testObject, "enumField", TestObject.BindPropertyEnum.HOURS.name());
        assertEquals(testObject.enumField, TestObject.BindPropertyEnum.HOURS);
    }

    @Test
    public void bind_enum_caseInsensitive() {
        bind0(testObject, "enumField", "dAyS");
        assertEquals(testObject.enumField, TestObject.BindPropertyEnum.DAYS);
    }

    @Test
    public void bind_enum_privateEnumClass() {
        bind0(testObject, "privateEnumField", TestObject.PrivateBindPropertyEnum.PRIVATE.name());
        assertEquals(testObject.privateEnumField, TestObject.PrivateBindPropertyEnum.PRIVATE);
    }

    @Test(expected = BindException.class)
    public void bind_enum_fieldNotFound() {
        bind0(testObject, "enumField", "notExist");
    }

    @SuppressWarnings({"unused", "WeakerAccess"})
    private static class TestObject {

        public enum BindPropertyEnum {
            DAYS,
            HOURS
        }

        private enum PrivateBindPropertyEnum {
            PRIVATE
        }

        public boolean booleanField;
        public Boolean booleanObjectField;

        public Object objectField;
        public String stringField;
        public BindPropertyEnum enumField;
        public PrivateBindPropertyEnum privateEnumField;

        public Class clazz;
    }
}
