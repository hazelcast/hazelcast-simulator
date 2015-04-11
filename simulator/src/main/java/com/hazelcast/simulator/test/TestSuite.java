/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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
package com.hazelcast.simulator.test;

import com.hazelcast.simulator.utils.BindException;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import static com.hazelcast.simulator.utils.CommonUtils.closeQuietly;
import static com.hazelcast.simulator.utils.FileUtils.isValidFileName;
import static java.lang.String.format;

public class TestSuite implements Serializable {

    private static final long serialVersionUID = 1;

    public final String id = new SimpleDateFormat("yyyy-MM-dd__HH_mm_ss").format(new Date());
    public final List<TestCase> testCaseList = new LinkedList<TestCase>();

    public Set<Failure.Type> tolerableFailures = Collections.emptySet();

    public int duration;
    public boolean failFast;

    public TestCase getTestCase(String testCaseId) {
        if (testCaseId == null) {
            return null;
        }

        for (TestCase testCase : testCaseList) {
            if (testCaseId.equals(testCase.getId())) {
                return testCase;
            }
        }
        return null;
    }

    public void addTest(TestCase testCase) {
        testCaseList.add(testCase);
    }

    public int size() {
        return testCaseList.size();
    }

    @Override
    public String toString() {
        return "TestSuite{"
                + "duration=" + duration
                + ", id='" + id + '\''
                + ", testRecipeList=" + testCaseList
                + ", failFast=" + failFast
                + '}';
    }

    public static TestSuite loadTestSuite(File testPropertiesFile, String propertiesOverrideString) {
        Properties properties = loadProperties(testPropertiesFile);

        Map<String, TestCase> testCases = createTestCases(properties);

        return createTestSuite(testPropertiesFile, testCases, propertiesOverrideString);
    }

    private static Properties loadProperties(File file) {
        FileReader reader = null;
        try {
            reader = new FileReader(file);

            Properties properties = new Properties();
            properties.load(reader);

            return properties;
        } catch (IOException e) {
            throw new RuntimeException(format("Failed to load testsuite property file [%s]", file.getAbsolutePath()), e);
        } finally {
            closeQuietly(reader);
        }
    }

    private static Map<String, TestCase> createTestCases(Properties properties) {
        Map<String, TestCase> testCases = new HashMap<String, TestCase>();
        for (String property : properties.stringPropertyNames()) {
            String value = (String) properties.get(property);
            int indexOfAt = property.indexOf("@");

            String testCaseId = "";
            String field = property;
            if (indexOfAt > -1) {
                testCaseId = property.substring(0, indexOfAt);
                field = property.substring(indexOfAt + 1);
            }

            TestCase testCase = getOrCreateTestCase(testCases, testCaseId);
            testCase.setProperty(field, value);
        }
        return testCases;
    }

    private static TestCase getOrCreateTestCase(Map<String, TestCase> testCases, String testCaseId) {
        TestCase testCase = testCases.get(testCaseId);
        if (testCase == null) {
            if (!testCaseId.isEmpty() && !isValidFileName(testCaseId)) {
                throw new IllegalArgumentException(format(
                        "Can't create TestCase: testId [%s] is an invalid filename for performance log", testCaseId
                ));
            }

            testCase = new TestCase();
            testCase.id = testCaseId;
            testCases.put(testCaseId, testCase);
        }
        return testCase;
    }

    private static TestSuite createTestSuite(File file, Map<String, TestCase> testCases, String propertiesOverrideString) {
        Map<String, String> propertiesOverride = parseProperties(propertiesOverrideString);

        TestSuite testSuite = new TestSuite();
        for (String testcaseId : getTestCaseIds(testCases)) {
            TestCase testcase = testCases.get(testcaseId);
            testcase.override(propertiesOverride);

            if (testcase.getClassname() == null) {
                String msg;
                if (testcaseId.isEmpty()) {
                    msg = format("There is no class set in property file [%s]. Add class=YourTestClass", file.getAbsolutePath());
                } else {
                    msg = format("There is no class set for test [%s] in property file [%s]. Add %s.class=YourTestClass",
                            testcaseId, file.getAbsolutePath(), testcaseId
                    );
                }
                throw new BindException(msg);
            }

            testSuite.addTest(testcase);
        }

        return testSuite;
    }

    private static Map<String, String> parseProperties(String overrideProperties) {
        overrideProperties = overrideProperties.trim();

        Map<String, String> result = new HashMap<String, String>();
        if (overrideProperties.isEmpty()) {
            return result;
        }

        for (String entry : overrideProperties.split(",")) {
            String[] keyValue = entry.split("=");
            result.put(keyValue[0], keyValue[1]);
        }
        return result;
    }

    private static List<String> getTestCaseIds(Map<String, TestCase> testCases) {
        List<String> testcaseIds = new LinkedList<String>(testCases.keySet());
        Collections.sort(testcaseIds);
        return testcaseIds;
    }
}
