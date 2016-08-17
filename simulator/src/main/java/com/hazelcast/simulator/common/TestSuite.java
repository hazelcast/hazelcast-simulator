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
package com.hazelcast.simulator.common;

import com.hazelcast.simulator.protocol.registry.TargetType;
import com.hazelcast.simulator.utils.BindException;
import com.hazelcast.simulator.utils.CommandLineExitException;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import static com.hazelcast.simulator.utils.CommonUtils.closeQuietly;
import static com.hazelcast.simulator.utils.FileUtils.isValidFileName;
import static java.lang.String.format;
import static java.util.Collections.singletonMap;

@SuppressWarnings(value = "checkstyle:methodcount")
public class TestSuite {

    private final List<TestCase> testCaseList = new LinkedList<TestCase>();
    private int durationSeconds;
    private int warmupDurationSeconds;
    private boolean waitForTestCase;
    private boolean failFast;
    private boolean parallel;
    private TargetType targetType;
    private int targetCount;
    private boolean verifyEnabled;
    private Set<FailureType> tolerableFailures = Collections.emptySet();

    public TestSuite setVerifyEnabled(boolean verifyEnabled) {
        this.verifyEnabled = verifyEnabled;
        return this;
    }

    public boolean isVerifyEnabled() {
        return verifyEnabled;
    }


    public TestSuite setParallel(boolean parallel) {
        this.parallel = parallel;
        return this;
    }

    public boolean isParallel() {
        return parallel;
    }

    public TestSuite setTargetType(TargetType targetType) {
        this.targetType = targetType;
        return this;
    }

    public TargetType getTargetType(boolean hasClientWorkers) {
        return targetType.resolvePreferClient(hasClientWorkers);
    }

    public TestSuite setTargetCount(int targetCount) {
        this.targetCount = targetCount;
        return this;
    }

    public int getTargetCount() {
        return targetCount;
    }

    public List<TestCase> getTestCaseList() {
        return testCaseList;
    }

    public TestSuite setDurationSeconds(int durationSeconds) {
        this.durationSeconds = durationSeconds;
        return this;
    }

    public TestSuite setWarmupDurationSeconds(int warmupDurationSeconds) {
        this.warmupDurationSeconds = warmupDurationSeconds;
        return this;
    }

    public int getWarmupDurationSeconds() {
        return warmupDurationSeconds;
    }

    public int getDurationSeconds() {
        return durationSeconds;
    }

    public TestSuite setWaitForTestCase(boolean waitForTestCase) {
        this.waitForTestCase = waitForTestCase;
        return this;
    }

    public boolean isWaitForTestCase() {
        return waitForTestCase;
    }

    public TestSuite setFailFast(boolean failFast) {
        this.failFast = failFast;
        return this;
    }

    public boolean isFailFast() {
        return failFast;
    }

    public TestSuite setTolerableFailures(Set<FailureType> tolerableFailures) {
        this.tolerableFailures = tolerableFailures;
        return this;
    }

    public Set<FailureType> getTolerableFailures() {
        return tolerableFailures;
    }

    public TestSuite addTest(TestCase testCase) {
        testCaseList.add(testCase);
        return this;
    }

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

    public int size() {
        return testCaseList.size();
    }

    public int getMaxTestCaseIdLength() {
        int maxLength = Integer.MIN_VALUE;
        for (TestCase testCase : testCaseList) {
            String testCaseId = testCase.getId();
            if (testCaseId != null && !testCaseId.isEmpty() && testCaseId.length() > maxLength) {
                maxLength = testCaseId.length();
            }
        }
        return (maxLength > 0) ? maxLength : 0;
    }

    @Override
    public String toString() {
        return "TestSuite{"
                + "durationSeconds=" + durationSeconds
                + ", warmupDurationSeconds=" + warmupDurationSeconds
                + ", waitForTestCase=" + waitForTestCase
                + ", failFast=" + failFast
                + ", parallel=" + parallel
                + ", targetType=" + targetType
                + ", targetCount=" + targetCount
                + ", verifyEnabled=" + verifyEnabled
                + ", tolerableFailures=" + tolerableFailures
                + ", testCaseList=" + testCaseList
                + '}';
    }

    public static TestSuite loadTestSuite(File testPropertiesFile, String propertiesOverrideString) {
        Properties properties = loadProperties(testPropertiesFile);

        Map<String, TestCase> testCases = createTestCases(properties);
        if (testCases.size() == 1) {
            // use classname instead of empty testId in single test scenarios
            TestCase testCase = testCases.values().iterator().next();
            String className = testCase.getClassname();
            if (testCase.getId().isEmpty() && className != null) {
                String testId = className.substring(className.lastIndexOf('.') + 1);
                testCases = singletonMap(testId, new TestCase(testId, testCase.getProperties()));
            }
        }

        return createTestSuite(testPropertiesFile, testCases, propertiesOverrideString);
    }

    @SuppressFBWarnings("DM_DEFAULT_ENCODING")
    private static Properties loadProperties(File file) {
        FileReader reader = null;
        try {
            reader = new FileReader(file);

            Properties properties = new Properties();
            properties.load(reader);

            return properties;
        } catch (IOException e) {
            throw new CommandLineExitException(format("Failed to load TestSuite property file [%s]", file.getAbsolutePath()), e);
        } finally {
            closeQuietly(reader);
        }
    }

    private static Map<String, TestCase> createTestCases(Properties properties) {
        Map<String, TestCase> testCases = new HashMap<String, TestCase>();
        for (String property : properties.stringPropertyNames()) {
            String value = (String) properties.get(property);

            int indexOfAt = property.indexOf('@');
            String testCaseId = "";
            String field = property;
            if (indexOfAt > -1) {
                testCaseId = property.substring(0, indexOfAt);
                field = property.substring(indexOfAt + 1);
            }

            if (value.isEmpty()) {
                throw new IllegalArgumentException(format("Value of property %s in testId [%s] is empty!", property, testCaseId));
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

            testCase = new TestCase(testCaseId);
            testCases.put(testCaseId, testCase);
        }
        return testCase;
    }

    private static TestSuite createTestSuite(File file, Map<String, TestCase> testCases, String propertiesOverrideString) {
        Map<String, String> propertiesOverride = parseProperties(propertiesOverrideString);

        TestSuite testSuite = new TestSuite();
        for (String testCaseId : getTestCaseIds(testCases)) {
            TestCase testCase = testCases.get(testCaseId);
            testCase.override(propertiesOverride);

            if (testCase.getClassname() == null) {
                String msg;
                if (testCaseId.isEmpty()) {
                    msg = format("There is no class set in property file [%s]. Add class=YourTestClass", file.getAbsolutePath());
                } else {
                    msg = format("There is no class set for test [%s] in property file [%s]. Add %s.class=YourTestClass",
                            testCaseId, file.getAbsolutePath(), testCaseId);
                }
                throw new BindException(msg);
            }

            testSuite.addTest(testCase);
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
        List<String> testCaseIds = new LinkedList<String>(testCases.keySet());
        Collections.sort(testCaseIds);
        return testCaseIds;
    }

}
