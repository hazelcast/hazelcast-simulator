package com.hazelcast.stabilizer.worker;

import com.hazelcast.stabilizer.common.messaging.Message;
import com.hazelcast.stabilizer.probes.probes.IntervalProbe;
import com.hazelcast.stabilizer.probes.probes.Probes;
import com.hazelcast.stabilizer.probes.probes.ProbesConfiguration;
import com.hazelcast.stabilizer.probes.probes.Result;
import com.hazelcast.stabilizer.probes.probes.SimpleProbe;
import com.hazelcast.stabilizer.probes.probes.impl.DisabledResult;
import com.hazelcast.stabilizer.test.TestContext;
import com.hazelcast.stabilizer.test.annotations.Name;
import com.hazelcast.stabilizer.test.annotations.Performance;
import com.hazelcast.stabilizer.test.annotations.Receive;
import com.hazelcast.stabilizer.test.annotations.Run;
import com.hazelcast.stabilizer.test.annotations.Setup;
import com.hazelcast.stabilizer.test.annotations.Teardown;
import com.hazelcast.stabilizer.test.annotations.Verify;
import com.hazelcast.stabilizer.test.annotations.Warmup;
import com.hazelcast.stabilizer.test.exceptions.IllegalTestException;
import com.hazelcast.stabilizer.worker.utils.AnnotationFilter.TeardownFilter;
import com.hazelcast.stabilizer.worker.utils.AnnotationFilter.VerifyFilter;
import com.hazelcast.stabilizer.worker.utils.AnnotationFilter.WarmupFilter;
import com.hazelcast.util.Clock;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.hazelcast.stabilizer.worker.utils.ReflectionUtils.getMandatoryVoidMethodWithoutArgs;
import static com.hazelcast.stabilizer.worker.utils.ReflectionUtils.getOptionalMethodWithoutArgs;
import static com.hazelcast.stabilizer.worker.utils.ReflectionUtils.getOptionalVoidMethodSkipArgsCheck;
import static com.hazelcast.stabilizer.worker.utils.ReflectionUtils.getOptionalVoidMethodWithoutArgs;
import static com.hazelcast.stabilizer.worker.utils.ReflectionUtils.injectObjectToInstance;
import static com.hazelcast.stabilizer.worker.utils.ReflectionUtils.invokeMethod;
import static java.lang.String.format;

/**
 * Since the test is based on annotations, there is no API we can call very easily.
 * That is the task of this test container.
 *
 * @param <T> Class of type TextContext
 */
public class TestContainer<T extends TestContext> {

    private final Object testClassInstance;
    private final Class<?> testClassType;
    private final T testContext;
    private final ProbesConfiguration probesConfiguration;

    private final Map<String, SimpleProbe<?, ?>> probeMap = new ConcurrentHashMap<String, SimpleProbe<?, ?>>();

    private Method runMethod;

    private Method setupMethod;
    private Object[] setupArguments;

    private Method localWarmupMethod;
    private Method globalWarmupMethod;

    private Method localVerifyMethod;
    private Method globalVerifyMethod;

    private Method localTeardownMethod;
    private Method globalTeardownMethod;

    private Method operationCountMethod;
    private Method messageConsumerMethod;

    public TestContainer(Object testObject, T testContext, ProbesConfiguration probesConfiguration) {
        if (testObject == null) {
            throw new NullPointerException();
        }
        if (testContext == null) {
            throw new NullPointerException();
        }

        this.testContext = testContext;
        this.testClassInstance = testObject;
        this.testClassType = testObject.getClass();
        this.probesConfiguration = probesConfiguration;

        initMethods();
    }

    public Map<String, Result<?>> getProbeResults() {
        Map<String, Result<?>> results = new HashMap<String, Result<?>>(probeMap.size());
        for (Map.Entry<String, SimpleProbe<?, ?>> entry : probeMap.entrySet()) {
            String name = entry.getKey();
            SimpleProbe<?, ?> probe = entry.getValue();
            Result<?> result = probe.getResult();
            if (!(result instanceof DisabledResult)) {
                results.put(name, result);
            }
        }
        return results;
    }

    public T getTestContext() {
        return testContext;
    }

    public void run() throws Throwable {
        long now = Clock.currentTimeMillis();
        for (SimpleProbe probe : probeMap.values()) {
            probe.startProbing(now);
        }
        invokeMethod(testClassInstance, runMethod);
        now = Clock.currentTimeMillis();
        for (SimpleProbe probe : probeMap.values()) {
            probe.stopProbing(now);
        }
    }

    public void setup() throws Throwable {
        invokeMethod(testClassInstance, setupMethod, setupArguments);
    }

    public void localWarmup() throws Throwable {
        invokeMethod(testClassInstance, localWarmupMethod);
    }

    public void globalWarmup() throws Throwable {
        invokeMethod(testClassInstance, globalWarmupMethod);
    }

    public void localVerify() throws Throwable {
        invokeMethod(testClassInstance, localVerifyMethod);
    }

    public void globalVerify() throws Throwable {
        invokeMethod(testClassInstance, globalVerifyMethod);
    }

    public void globalTeardown() throws Throwable {
        invokeMethod(testClassInstance, globalTeardownMethod);
    }

    public void localTeardown() throws Throwable {
        invokeMethod(testClassInstance, localTeardownMethod);
    }

    public long getOperationCount() throws Throwable {
        Long count = invokeMethod(testClassInstance, operationCountMethod);
        return (count == null ? -1 : count);
    }

    public void sendMessage(Message message) throws Throwable {
        invokeMethod(testClassInstance, messageConsumerMethod, message);
    }

    private void initMethods() {
        runMethod = getMandatoryVoidMethodWithoutArgs(testClassType, Run.class);

        setupMethod = getOptionalVoidMethodSkipArgsCheck(testClassType, Setup.class);
        if (setupMethod != null) {
            assertSetupArguments(setupMethod);
            setupArguments = getSetupArguments(setupMethod);
        }

        localWarmupMethod = getOptionalVoidMethodWithoutArgs(testClassType, Warmup.class, new WarmupFilter(false));
        globalWarmupMethod = getOptionalVoidMethodWithoutArgs(testClassType, Warmup.class, new WarmupFilter(true));

        localVerifyMethod = getOptionalVoidMethodWithoutArgs(testClassType, Verify.class, new VerifyFilter(false));
        globalVerifyMethod = getOptionalVoidMethodWithoutArgs(testClassType, Verify.class, new VerifyFilter(true));

        localTeardownMethod = getOptionalVoidMethodWithoutArgs(testClassType, Teardown.class, new TeardownFilter(false));
        globalTeardownMethod = getOptionalVoidMethodWithoutArgs(testClassType, Teardown.class, new TeardownFilter(true));

        operationCountMethod = getOptionalMethodWithoutArgs(testClassType, Performance.class, Long.TYPE);
        messageConsumerMethod = getOptionalVoidMethodSkipArgsCheck(testClassType, Receive.class);
        if (messageConsumerMethod != null) {
            assertArguments(messageConsumerMethod, Message.class);
        }

        injectDependencies();
    }

    private void assertSetupArguments(Method method) {
        Class<?>[] parameterTypes = method.getParameterTypes();
        if (parameterTypes.length < 1) {
            return;
        }

        boolean testContextFound = false;
        boolean illegalArgumentFound = false;
        for (Class<?> parameterType : parameterTypes) {
            boolean isObject = parameterType.isAssignableFrom(Object.class);
            if (!isObject && parameterType.isAssignableFrom(TestContext.class)) {
                testContextFound = true;
            } else if (!parameterType.isAssignableFrom(IntervalProbe.class) || isObject) {
                illegalArgumentFound = true;
                break;
            }
        }
        if (!testContextFound || illegalArgumentFound) {
            throw new IllegalTestException(
                    format("Method %s.%s must have argument of type %s and zero or more arguments of type %s", testClassType,
                            method, TestContext.class, SimpleProbe.class));
        }
    }

    private void assertArguments(Method method, Class<?>... arguments) {
        Class<?>[] parameterTypes = method.getParameterTypes();
        if (parameterTypes.length != arguments.length) {
            throw new IllegalTestException(
                    format("Method %s must have %s arguments, but %s arguments found", method, arguments.length,
                            parameterTypes.length));
        }

        for (int i = 0; i < arguments.length; i++) {
            if (!parameterTypes[i].isAssignableFrom(arguments[i])) {
                throw new IllegalTestException(
                        format("Method %s has argument of type %s at index %d where type %s is expected", method,
                                parameterTypes[i], i + 1, arguments[i]));
            }
        }
    }

    private Object[] getSetupArguments(Method method) {
        Class<?>[] parameterTypes = method.getParameterTypes();
        Annotation[][] parameterAnnotations = method.getParameterAnnotations();
        Object[] arguments = new Object[parameterTypes.length];
        for (int i = 0; i < parameterTypes.length; i++) {
            Class<?> parameterType = parameterTypes[i];
            arguments[i] = getSetupArgumentForParameterType(i, parameterType, parameterAnnotations[i]);
        }
        return arguments;
    }

    private Object getSetupArgumentForParameterType(int index, Class<?> parameterType, Annotation[] parameterAnnotations) {
        if (parameterType.equals(IntervalProbe.class)) {
            String probeName = getProbeName(parameterAnnotations, index);
            return getOrCreateProbe(probeName, IntervalProbe.class);
        }
        if (parameterType.equals(SimpleProbe.class)) {
            String probeName = getProbeName(parameterAnnotations, index);
            SimpleProbe probe = getOrCreateProbe(probeName, SimpleProbe.class);
            probeMap.put(probeName, probe);
            return probe;
        }
        if (parameterType.isAssignableFrom(TestContext.class)) {
            return testContext;
        }
        throw new IllegalArgumentException(format("Unknown parameter type %s at index %s in setup method", parameterType, index));
    }

    private void injectDependencies() {
        Field[] fields = testClassType.getDeclaredFields();
        for (Field field : fields) {
            String name = getProbeName(field);
            if (SimpleProbe.class.equals(field.getType())) {
                SimpleProbe probe = getOrCreateProbe(name, SimpleProbe.class);
                injectObjectToInstance(testClassInstance, field, probe);
            } else if (IntervalProbe.class.equals(field.getType())) {
                IntervalProbe probe = getOrCreateProbe(name, IntervalProbe.class);
                injectObjectToInstance(testClassInstance, field, probe);
            }
        }
    }

    private String getProbeName(Annotation[] parameterType, int index) {
        for (Annotation annotation : parameterType) {
            if (annotation.annotationType().equals(Name.class)) {
                Name name = (Name) annotation;
                return name.value();
            }
        }
        return "Probe" + index;
    }

    private String getProbeName(Field field) {
        Name nameAnnotation = field.getAnnotation(Name.class);
        if (nameAnnotation != null) {
            return nameAnnotation.value();
        }
        return field.getName();
    }

    @SuppressWarnings("unchecked")
    private <P extends SimpleProbe> P getOrCreateProbe(String probeName, Class<P> probeType) {
        SimpleProbe<?, ?> probe = probeMap.get(probeName);
        if (probe == null) {
            probe = Probes.createProbe(probeType, probeName, probesConfiguration);
            probeMap.put(probeName, probe);
            return (P) probe;
        }
        if (probeType.isAssignableFrom(probe.getClass())) {
            return (P) probe;
        }
        throw new IllegalArgumentException(
                format("Can't create a probe %s of type %s as there is already a probe %s with the same name", probeName,
                        probeType.getName(), probe.getClass()));
    }
}
