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
package com.hazelcast.simulator.coordinator;

import com.amazonaws.util.StringInputStream;
import com.hazelcast.simulator.common.TestCase;
import com.hazelcast.simulator.protocol.registry.WorkerQuery;
import com.hazelcast.simulator.utils.BindException;
import com.hazelcast.simulator.utils.CommandLineExitException;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import static com.hazelcast.simulator.utils.CommonUtils.rethrow;
import static com.hazelcast.simulator.utils.FileUtils.fileAsText;
import static com.hazelcast.simulator.utils.FileUtils.isValidFileName;
import static java.lang.String.format;
import static java.util.Collections.singletonMap;

@SuppressWarnings(value = "checkstyle:methodcount")
public class TestSuite {

    private static final Pattern VALID_FILE_NAME_PATTERN = Pattern.compile("^[a-zA-Z0-9-]+$");

    private final List<TestCase> testCaseList = new LinkedList<TestCase>();
    private int durationSeconds;
    private boolean failFast;
    private boolean parallel;
    // a 'select all' workerQuery by default
    private WorkerQuery workerQuery = new WorkerQuery();
    private boolean verifyEnabled;

    public TestSuite() {
    }

    public TestSuite(File file) {
        if (!file.exists()) {
            throw new CommandLineExitException(format("TestSuite [%s] does not exist", file));
        }

        try {
            String testContent = fileAsText(file);
            load(testContent);
        } catch (BindException e) {
            String msg = format("Failed to load file [%s]. %s", file.getAbsoluteFile(), e.getMessage());
            BindException bindException = new BindException(msg);
            bindException.setStackTrace(e.getStackTrace());
            throw e;
        }
    }

    public TestSuite(String content) {
        load(content);
    }

    @SuppressFBWarnings("DM_DEFAULT_ENCODING")
    private void load(String testContent) {
        Properties properties = new Properties();
        try {
            InputStream in = new StringInputStream(testContent);
            properties.load(in);
        } catch (UnsupportedEncodingException e) {
            throw rethrow(e);
        } catch (IOException e) {
            throw rethrow(e);
        }

        Map<String, TestCase> testCases = createTestCases(properties);
        if (testCases.isEmpty()) {
            throw new BindException("No tests are defined.");
        }

        if (testCases.size() == 1) {
            // use classname instead of empty desc in single test scenarios
            TestCase testCase = testCases.values().iterator().next();
            String className = testCase.getClassname();
            if (testCase.getId().isEmpty() && className != null) {
                String desc = className.substring(className.lastIndexOf('.') + 1);
                testCases = singletonMap(desc, new TestCase(desc, testCase.getProperties()));
            }
        }

        for (String testCaseId : getTestCaseIds(testCases)) {
            TestCase testCase = testCases.get(testCaseId);

            if (testCase.getClassname() == null) {
                String msg;
                if (testCaseId.isEmpty()) {
                    msg = "There is no class set. Add class=YourTestClass";
                } else {
                    msg = format("There is no class set for test [%s]. Add %s.class=YourTestClass", testCaseId, testCaseId);
                }
                throw new BindException(msg);
            }

            addTest(testCase);
        }
    }

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

    public WorkerQuery getWorkerQuery() {
        return workerQuery;
    }

    public TestSuite setWorkerQuery(WorkerQuery workerQuery) {
        this.workerQuery = workerQuery;
        return this;
    }

    public List<TestCase> getTestCaseList() {
        return testCaseList;
    }

    public TestSuite setDurationSeconds(int durationSeconds) {
        this.durationSeconds = durationSeconds;
        return this;
    }

    public int getDurationSeconds() {
        return durationSeconds;
    }

    public TestSuite setFailFast(boolean failFast) {
        this.failFast = failFast;
        return this;
    }

    public boolean isFailFast() {
        return failFast;
    }

    public TestSuite addTest(TestCase testCase) {
        testCaseList.add(testCase);
        return this;
    }

    public TestSuite setWarmupSeconds(long warmupSeconds) {
        for (TestCase testCase : testCaseList) {
            testCase.setWarmupMillis(TimeUnit.SECONDS.toMillis(warmupSeconds));
        }
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
            String testId = testCase.getId();
            if (!testId.isEmpty() && testId.length() > maxLength) {
                maxLength = testId.length();
            }
        }
        return maxLength > 0 ? maxLength : 0;
    }

    @Override
    public String toString() {
        return "TestSuite{"
                + "durationSeconds=" + durationSeconds
                + ", failFast=" + failFast
                + ", parallel=" + parallel
                + ", verifyEnabled=" + verifyEnabled
                + ", workerQuery=" + workerQuery
                + ", testCaseList=" + testCaseList
                + '}';
    }

    private static Map<String, TestCase> createTestCases(Properties properties) {
        Map<String, TestCase> testCases = new HashMap<String, TestCase>();
        for (String property : properties.stringPropertyNames()) {
            String value = (String) properties.get(property);

            int indexOfAt = property.indexOf('@');
            String testId = "";
            String field = property;
            if (indexOfAt > -1) {
                testId = property.substring(0, indexOfAt);
                field = property.substring(indexOfAt + 1);
            }

            if (!testId.isEmpty() && !isValidTestId(testId)) {
                throw new IllegalArgumentException(format("TestId [%s] is not a valid id", testId));
            }

            if (value.isEmpty()) {
                throw new IllegalArgumentException(format("Value of property %s in testId [%s] is empty!", property, testId));
            }

            TestCase testCase = getOrCreateTestCase(testCases, testId);
            testCase.setProperty(field, value);
        }
        return testCases;
    }

    private static boolean isValidTestId(String fileName) {
        return VALID_FILE_NAME_PATTERN.matcher(fileName).matches();
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
