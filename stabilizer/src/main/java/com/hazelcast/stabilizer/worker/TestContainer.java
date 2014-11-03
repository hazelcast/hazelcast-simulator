package com.hazelcast.stabilizer.worker;

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.stabilizer.common.messaging.Message;
import com.hazelcast.stabilizer.probes.probes.IntervalProbe;
import com.hazelcast.stabilizer.probes.probes.ProbesConfiguration;
import com.hazelcast.stabilizer.probes.probes.Result;
import com.hazelcast.stabilizer.probes.probes.SimpleProbe;
import com.hazelcast.stabilizer.probes.probes.impl.DisabledResult;
import com.hazelcast.stabilizer.tests.IllegalTestException;
import com.hazelcast.stabilizer.tests.TestContext;
import com.hazelcast.stabilizer.tests.annotations.Name;
import com.hazelcast.stabilizer.tests.annotations.Performance;
import com.hazelcast.stabilizer.tests.annotations.Receive;
import com.hazelcast.stabilizer.tests.annotations.Run;
import com.hazelcast.stabilizer.tests.annotations.Setup;
import com.hazelcast.stabilizer.tests.annotations.Teardown;
import com.hazelcast.stabilizer.tests.annotations.Verify;
import com.hazelcast.stabilizer.tests.annotations.Warmup;
import com.hazelcast.util.Clock;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import com.hazelcast.stabilizer.probes.probes.Probes;

import static java.lang.String.format;

/**
 * Since the test is based on annotations, there is no API we can call very easily.
 * That is the task of this test container.
 *
 * @param <T>
 */
public class TestContainer<T extends TestContext> {

    private final static ILogger log = Logger.getLogger(TestContainer.class);

    private final Object testObject;
    private final Class<? extends Object> clazz;
    private final T testContext;
    private final ProbesConfiguration probesConfiguration;

    private Method runMethod;
    private Method setupMethod;

    private Method localTeardownMethod;
    private Method globalTeardownMethod;

    private Method localWarmupMethod;
    private Method globalWarmupMethod;

    private Method localVerifyMethod;
    private Method globalVerifyMethod;

    private Method operationCountMethod;
    private Method messageConsumerMethod;

    private Map<String, SimpleProbe<?, ?>> probeMap = new ConcurrentHashMap<String, SimpleProbe<?, ?>>();
    private Object[] setupArguments;

    public TestContainer(Object testObject, T testContext, ProbesConfiguration probesConfiguration) {
        if (testObject == null) {
            throw new NullPointerException();
        }
        if (testContext == null) {
            throw new NullPointerException();
        }

        this.testContext = testContext;
        this.testObject = testObject;
        this.clazz = testObject.getClass();
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

    private void initMethods() {
        initRunMethod();
        initSetupMethod();

        initLocalTeardownMethod();
        initGlobalTeardownMethod();

        initLocalWarmupMethod();
        initGlobalWarmupMethod();

        initLocalVerifyMethod();
        initGlobalVerifyMethod();

        initGetOperationCountMethod();

        initMessageConsumerMethod();
        injectDependencies();
    }

    public T getTestContext() {
        return testContext;
    }

    public long getOperationCount() throws Throwable {
        Long count = invoke(operationCountMethod);
        return count == null ? -1 : count;
    }

    public void run() throws Throwable {
        long now = Clock.currentTimeMillis();
        for (SimpleProbe probe : probeMap.values()) {
            probe.startProbing(now);
        }
        invoke(runMethod);
        now = Clock.currentTimeMillis();
        for (SimpleProbe probe : probeMap.values()) {
            probe.stopProbing(now);
        }
    }

    public void setup() throws Throwable {
        invoke(setupMethod, setupArguments);
    }

    public void globalTeardown() throws Throwable {
        invoke(globalTeardownMethod);
    }

    public void localTeardown() throws Throwable {
        invoke(localTeardownMethod);
    }

    public void localVerify() throws Throwable {
        invoke(localVerifyMethod);
    }

    public void globalVerify() throws Throwable {
        invoke(globalVerifyMethod);
    }

    public void localWarmup() throws Throwable {
        invoke(localWarmupMethod);
    }

    public void globalWarmup() throws Throwable {
        invoke(globalWarmupMethod);
    }

    public void sendMessage(Message message) throws Throwable {
        invoke(messageConsumerMethod, message);
    }

    private <E> E invoke(Method method, Object... args) throws Throwable {
        if (method == null) {
            return null;
        }

        try {
            return (E) method.invoke(testObject, args);
        } catch (InvocationTargetException e) {
            throw e.getCause();
        }
    }

    private void initSetupMethod() {
        List<Method> methods = findMethod(Setup.class);
        assertAtMostOne(methods, Setup.class);

        if (methods.isEmpty()) {
            return;
        }

        Method method = methods.get(0);
        method.setAccessible(true);
        assertNotStatic(method);
        assertVoidReturnType(method);
        assertSetupArguments(method);

        initSetupArguments(method);
        setupMethod = method;
    }

    private void injectDependencies() {
        Field[] fields = clazz.getDeclaredFields();
        for (Field field : fields) {
            String name = getProbeName(field);
            if (SimpleProbe.class.equals(field.getType())) {
                SimpleProbe probe = getOrCreateProbe(name, SimpleProbe.class);
                injectObjectToTest(field, probe);
            } else if (IntervalProbe.class.equals(field.getType())) {
                IntervalProbe probe = getOrCreateProbe(name, IntervalProbe.class);
                injectObjectToTest(field, probe);
            }
        }
    }

    private String getProbeName(Field field) {
        Name nameAnnotation = field.getAnnotation(Name.class);
        if (nameAnnotation != null) {
            return nameAnnotation.value();
        }
        return field.getName();
    }

    private void injectObjectToTest(Field field, Object object) {
        field.setAccessible(true);
        try {
            field.set(testObject, object);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    private void initSetupArguments(Method method) {
        Class<?>[] parameterTypes = method.getParameterTypes();
        Annotation[][] parameterAnnotations = method.getParameterAnnotations();
        setupArguments = new Object[parameterTypes.length];
        for (int i = 0; i < parameterTypes.length; i++) {
            Class<?> parameterType = parameterTypes[i];
            initSetupArgument(i, parameterType, parameterAnnotations[i]);
        }
    }

    private void initSetupArgument(int i, Class<?> parameterType, Annotation[] parameterAnnotations) {
        if (parameterType.equals(IntervalProbe.class)) {
            String probeName = getProbeName(parameterAnnotations, i);
            IntervalProbe probe = getOrCreateProbe(probeName, IntervalProbe.class);
            setupArguments[i] = probe;
        } else if (parameterType.equals(SimpleProbe.class)) {
            String probeName = getProbeName(parameterAnnotations, i);
            SimpleProbe probe = getOrCreateProbe(probeName, SimpleProbe.class);
            probeMap.put(probeName, probe);
            setupArguments[i] = probe;
        } else if (parameterType.isAssignableFrom(TestContext.class)) {
            setupArguments[i] = testContext;
        }
    }

    private <T extends SimpleProbe> T getOrCreateProbe(String probeName, Class<T> probeType) {
        SimpleProbe<?, ?> probe = probeMap.get(probeName);
        if (probe == null) {
            probe = Probes.createProbe(probeType, probeName, probesConfiguration);
            probeMap.put(probeName, probe);
            return (T) probe;
        }
        if (probeType.isAssignableFrom(probe.getClass())) {
            return (T) probe;
        }
        throw new IllegalArgumentException("Can't create a probe " + probeName + " of type " + probeType.getName() + " as " +
                "there is already a probe " + probe.getClass() + " with the same name");

    }

    private String getProbeName(Annotation[] parameterType, int i) {
        for (Annotation annotation : parameterType) {
            if (annotation.annotationType().equals(Name.class)) {
                Name name = (Name) annotation;
                return name.value();
            }
        }
        return "Probe" + i;
    }

    private void initGetOperationCountMethod() {
        List<Method> methods = findMethod(Performance.class);
        assertAtMostOne(methods, Performance.class);

        if (methods.isEmpty()) {
            return;
        }

        Method method = methods.get(0);
        method.setAccessible(true);
        assertNotStatic(method);
        assertNoArgs(method);
        assertReturnType(method, Long.TYPE);
        operationCountMethod = method;
    }

    private void initRunMethod() {
        List<Method> methods = findMethod(Run.class);
        assertExactlyOne(methods, Run.class);

        Method method = methods.get(0);
        method.setAccessible(true);
        assertVoidReturnType(method);
        assertNotStatic(method);
        assertNoArgs(method);
        runMethod = method;
    }

    private void initLocalVerifyMethod() {
        List<Method> methods = findMethod(Verify.class, new Filter<Verify>() {
            @Override
            public boolean allowed(Verify t) {
                return !t.global();
            }
        });

        if (methods.isEmpty()) {
            return;
        }

        assertAtMostOne(methods, Verify.class);
        Method method = methods.get(0);
        method.setAccessible(true);
        assertVoidReturnType(method);
        assertNotStatic(method);
        assertNoArgs(method);
        localVerifyMethod = method;
    }

    private void initGlobalVerifyMethod() {
        List<Method> methods = findMethod(Verify.class, new Filter<Verify>() {
            @Override
            public boolean allowed(Verify t) {
                return t.global();
            }
        });

        if (methods.isEmpty()) {
            return;
        }

        assertAtMostOne(methods, Verify.class);
        Method method = methods.get(0);
        method.setAccessible(true);
        assertVoidReturnType(method);
        assertNotStatic(method);
        assertNoArgs(method);
        globalVerifyMethod = method;
    }

    private void initLocalTeardownMethod() {
        List<Method> methods = findMethod(Teardown.class, new Filter<Teardown>() {
            @Override
            public boolean allowed(Teardown t) {
                return !t.global();
            }
        });

        if (methods.isEmpty()) {
            return;
        }

        assertAtMostOne(methods, Teardown.class);
        Method method = methods.get(0);
        method.setAccessible(true);
        assertVoidReturnType(method);
        assertNotStatic(method);
        assertNoArgs(method);
        localTeardownMethod = method;
    }

    private void initGlobalTeardownMethod() {
        List<Method> methods = findMethod(Teardown.class, new Filter<Teardown>() {
            @Override
            public boolean allowed(Teardown t) {
                return t.global();
            }
        });

        if (methods.isEmpty()) {
            return;
        }

        assertAtMostOne(methods, Teardown.class);
        Method method = methods.get(0);
        method.setAccessible(true);
        assertVoidReturnType(method);
        assertNotStatic(method);
        assertNoArgs(method);
        globalTeardownMethod = method;
    }

    private void initLocalWarmupMethod() {
        List<Method> methods = findMethod(Warmup.class, new Filter<Warmup>() {
            @Override
            public boolean allowed(Warmup t) {
                return !t.global();
            }
        });

        if (methods.isEmpty()) {
            return;
        }

        assertAtMostOne(methods, Warmup.class);
        Method method = methods.get(0);
        method.setAccessible(true);
        assertVoidReturnType(method);
        assertNotStatic(method);
        assertNoArgs(method);
        localWarmupMethod = method;
    }

    private void initGlobalWarmupMethod() {
        List<Method> methods = findMethod(Warmup.class, new Filter<Warmup>() {
            @Override
            public boolean allowed(Warmup t) {
                return t.global();
            }
        });

        if (methods.isEmpty()) {
            return;
        }

        assertAtMostOne(methods, Warmup.class);
        Method method = methods.get(0);
        method.setAccessible(true);
        assertVoidReturnType(method);
        assertNotStatic(method);
        assertNoArgs(method);
        globalWarmupMethod = method;
    }

    private void initMessageConsumerMethod() {
        List<Method> methods = findMethod(Receive.class);
        if (methods.isEmpty()) {
            return;
        }

        assertAtMostOne(methods, Receive.class);
        Method method = methods.get(0);
        method.setAccessible(true);
        assertVoidReturnType(method);
        assertNotStatic(method);
        assertArguments(method, Message.class);
        messageConsumerMethod = method;

    }

    private void assertNotStatic(Method method) {
        if (Modifier.isStatic(method.getModifiers())) {
            throw new IllegalTestException(
                    format("Method  %s can't be static", method.getName()));

        }
    }

    private void assertNoArgs(Method method) {
        if (method.getParameterTypes().length == 0) {
            return;
        }

        throw new IllegalTestException(format("Method '%s' can't have any args", method));
    }

    private void assertSetupArguments(Method method) {
        Class<?>[] parameterTypes = method.getParameterTypes();
        if (parameterTypes.length < 1) {
            return;
        }

        boolean testContextFound = false;
        for (Class<?> parameterType : parameterTypes) {
            if (parameterType.isAssignableFrom(TestContext.class)) {
                testContextFound = true;
            } else if (!parameterType.isAssignableFrom(IntervalProbe.class)) {
                throw new IllegalTestException("Method " + clazz + "." + method + " must have argument of type "
                        + TestContext.class + " and zero or more arguments of type " + SimpleProbe.class);
            }
        }
        if (!testContextFound) {
            throw new IllegalTestException("Method " + clazz + "." + method + " must have argument of type " + TestContext.class
                    + " and zero or more arguments of type " + SimpleProbe.class);
        }
    }

    private void assertArguments(Method method, Class<?>... arguments) {
        Class<?>[] parameterTypes = method.getParameterTypes();
        if (parameterTypes.length != arguments.length) {
            throw new IllegalTestException(
                    format("Method %s must have %s arguments, but %s arguments found",
                            method, arguments.length, parameterTypes.length));
        }

        for (int i = 0; i < arguments.length; i++) {
            if (!parameterTypes[i].isAssignableFrom(arguments[i])) {
                throw new IllegalTestException(
                        format("Method %s has %s. argument of type %s where type %s is expected",
                                method, i + 1, parameterTypes[i], arguments[i]));
            }
        }
    }

    private void assertExactlyOne(List<Method> methods, Class<? extends Annotation> annotation) {
        if (methods.size() == 0) {
            throw new IllegalTestException(
                    format("No method annotated with %s found on class %s", annotation.getName(), clazz.getName()));
        } else if (methods.size() == 1) {
            return;
        } else {
            throw new IllegalTestException(
                    format("Too many methods on class %s with annotation %s", clazz.getName(), annotation.getName()));
        }
    }

    private void assertAtMostOne(List<Method> methods, Class<? extends Annotation> annotation) {
        if (methods.size() > 1) {
            throw new IllegalTestException(
                    format("Too many methods on class %s with annotation %s", clazz.getName(), annotation.getName()));
        }
    }

    private void assertVoidReturnType(Method method) {
        assertReturnType(method, Void.TYPE);
    }

    private void assertReturnType(Method method, Class expectedType) {
        if (expectedType.equals(method.getReturnType())) {
            return;
        }

        throw new IllegalTestException("Method " + clazz + "." + method + " should have returnType: " + expectedType);
    }

    private List<Method> findMethod(Class<? extends Annotation> annotation) {
        return findMethod(annotation, new AlwaysFilter());
    }

    private List<Method> findMethod(Class<? extends Annotation> annotation, Filter filter) {
        List<Method> methods = new LinkedList<Method>();

        for (Method method : clazz.getDeclaredMethods()) {
            Annotation found = method.getAnnotation(annotation);
            if (found != null && filter.allowed(found)) {
                methods.add(method);
            }
        }

        return methods;
    }

    private interface Filter<A extends Annotation> {
        boolean allowed(A m);
    }

    private class AlwaysFilter implements Filter {
        @Override
        public boolean allowed(Annotation m) {
            return true;
        }
    }
}
