package com.hazelcast.stabilizer.tests.utils;

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.stabilizer.tests.IllegalTestException;
import com.hazelcast.stabilizer.tests.TestContext;
import com.hazelcast.stabilizer.tests.annotations.Run;
import com.hazelcast.stabilizer.tests.annotations.Setup;
import com.hazelcast.stabilizer.tests.annotations.Teardown;
import com.hazelcast.stabilizer.tests.annotations.Verify;
import com.hazelcast.stabilizer.tests.annotations.Warmup;

import java.lang.annotation.Annotation;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.LinkedList;
import java.util.List;

import static java.lang.String.format;

public class TestInvoker<T extends TestContext> {

    private final static ILogger log = Logger.getLogger(TestInvoker.class);

    private final Object object;
    private final Class<? extends Object> clazz;
    private final T testContext;
    private Method runMethod;
    private Method setupMethod;

    private Method localTeardownMethod;
    private Method globalTeardownMethod;

    private Method localWarmupMethod;
    private Method globalWarmupMethod;

    private Method localVerifyMethod;
    private Method globalVerifyMethod;

    public TestInvoker(Object object, T testContext) {
        if (object == null) {
            throw new NullPointerException();
        }
        if (testContext == null) {
            throw new NullPointerException();
        }

        this.testContext = testContext;
        this.object = object;
        this.clazz = object.getClass();

        initRunMethod();
        initSetupMethod();

        initLocalTeardownMethod();
        initGlobalTeardownMethod();

        initLocalWarmupMethod();
        initGlobalWarmupMethod();

        initLocalVerifyMethod();
        initGlobalVerifyMethod();
    }

    public T getTestContext() {
        return testContext;
    }

    public void run() throws Throwable {
        invoke(runMethod);
    }

    public void setup() throws Throwable {
        invoke(setupMethod, testContext);
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

    private Object invoke(Method method, Object... args) throws Throwable {
        if (method == null) {
            return null;
        }

        try {
            return method.invoke(object, args);
        } catch (InvocationTargetException e) {
            throw e.getCause();
        }
    }

    private void initSetupMethod() {
        List<Method> methods = findMethod(Setup.class);
        assertExactlyOne(methods, Setup.class);

        Method method = methods.get(0);
        method.setAccessible(true);
        assertNotStatic(method);
        assertVoidReturnType(method);
        assertTestContextArgument(method);
        setupMethod = method;
    }

    private void initRunMethod() {
        List<Method> methods = findMethod(Run.class);
        assertExactlyOne(methods, Run.class);

        Method method = methods.get(0);
        method.setAccessible(true);
        assertVoidReturnType(method);
        assertNotStatic(method);
        assertNoArg(method);
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
        assertNoArg(method);
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
        assertNoArg(method);
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
        assertNoArg(method);
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
        assertNoArg(method);
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
        assertNoArg(method);
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
        assertNoArg(method);
        globalWarmupMethod = method;
    }

    private void assertNotStatic(Method method) {
        if (Modifier.isStatic(method.getModifiers())) {
            throw new IllegalTestException(
                    format("Method  %s can't be static", method.getName()));

        }
    }

    private void assertNoArg(Method method) {
        if (method.getParameterTypes().length == 0) {
            return;
        }

        throw new IllegalTestException(format("Method '%s' can't have any args", method));
    }

    private void assertTestContextArgument(Method method) {
        if (method.getParameterTypes().length == 1) {
            return;
        }

        if (TestContext.class.equals(method.getParameterTypes()[0])) {
            return;
        }

        throw new IllegalTestException(
                "Method " + clazz + "." + method + " should have single argument of type " + TestContext.class);
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
        if (Void.TYPE.equals(method.getReturnType())) {
            return;
        }

        throw new IllegalTestException("Method " + clazz + "." + method + " should have a void return type");
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
