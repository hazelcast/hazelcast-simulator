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

package com.hazelcast.simulator.worker.testcontainer;

import com.hazelcast.simulator.probes.LatencyProbe;
import com.hazelcast.simulator.worker.metronome.EmptyMetronome;
import com.hazelcast.simulator.worker.metronome.Metronome;
import freemarker.ext.util.WrapperTemplateModel;
import freemarker.template.Configuration;
import freemarker.template.SimpleNumber;
import freemarker.template.SimpleScalar;
import freemarker.template.Template;
import freemarker.template.TemplateExceptionHandler;
import freemarker.template.TemplateMethodModelEx;
import freemarker.template.TemplateModelException;

import javax.tools.Diagnostic;
import javax.tools.DiagnosticCollector;
import javax.tools.JavaCompiler;
import javax.tools.JavaFileObject;
import javax.tools.SimpleJavaFileObject;
import javax.tools.ToolProvider;
import java.io.File;
import java.io.StringWriter;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.net.URLClassLoader;
import java.security.PrivilegedAction;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.hazelcast.simulator.utils.FileUtils.ensureExistingDirectory;
import static com.hazelcast.simulator.utils.FileUtils.getUserDir;
import static com.hazelcast.simulator.utils.FileUtils.writeText;
import static java.security.AccessController.doPrivileged;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;

class TimeStepLoopCodeGenerator {

    private final JavaCompiler javaCompiler = ToolProvider.getSystemJavaCompiler();
    private final File targetDirectory = new File(getUserDir(), "timestep-loop-classes");

    Class compile(
            String testCaseId,
            String executionGroup,
            TimeStepModel timeStepModel,
            Class<? extends Metronome> metronomeClass,
            Class<? extends LatencyProbe> probeClass,
            long logFrequency,
            long logRateMs,
            boolean hasIterationCap) {

        ensureExistingDirectory(targetDirectory);

        String className = timeStepModel.getTestClass().getSimpleName();
        if (!"".equals(executionGroup)) {
            className += "_" + executionGroup + "_";

        }
        className += "Loop";

        if (!"".equals(testCaseId)) {
            className += testCaseId;
        }
        JavaFileObject file = createJavaFileObject(
                className, executionGroup, metronomeClass, timeStepModel, probeClass, logFrequency, logRateMs, hasIterationCap);
        return compile(javaCompiler, file, className);
    }

    Class compile(JavaCompiler compiler, JavaFileObject file, final String className) {
        if (compiler == null) {
            throw new IllegalStateException("Could not get Java compiler in TimeStepLoopCodeGenerator."
                    + " You need to use a JDK to run Simulator! Version found: " + System.getProperty("java.version"));
        }

        DiagnosticCollector<JavaFileObject> diagnostics = new DiagnosticCollector<>();
        JavaCompiler.CompilationTask task = compiler.getTask(
                null,
                null,
                diagnostics,
                asList("-d", targetDirectory.getAbsolutePath()),
                null,
                singletonList(file));

        boolean success = task.call();
        if (!success) {
            StringBuilder sb = new StringBuilder();
            for (Diagnostic diagnostic : diagnostics.getDiagnostics()) {
                sb.append("Error on line ")
                        .append(diagnostic.getLineNumber())
                        .append(" in ")
                        .append(diagnostic)
                        .append('\n');
            }
            throw new IllegalTestException(sb.toString());
        }

        return (Class) doPrivileged((PrivilegedAction) () -> {
            try {
                URLClassLoader classLoader = new URLClassLoader(new URL[]{targetDirectory.toURI().toURL()});
                return (Class) classLoader.loadClass(className);
            } catch (ClassNotFoundException | MalformedURLException e) {
                throw new IllegalTestException(e.getMessage(), e);
            }
        });
    }

    private JavaFileObject createJavaFileObject(
            String className,
            String executionGroup,
            Class<? extends Metronome> metronomeClass,
            TimeStepModel timeStepModel,
            Class<? extends LatencyProbe> probeClass,
            long logFrequency,
            long logRateMs,
            boolean hasIterationCap) {
        try {
            Configuration cfg = new Configuration(Configuration.VERSION_2_3_24);
            cfg.setClassForTemplateLoading(this.getClass(), "/");
            cfg.setDefaultEncoding("UTF-8");
            cfg.setTemplateExceptionHandler(TemplateExceptionHandler.RETHROW_HANDLER);
            cfg.setLogTemplateExceptions(false);

            Map<String, Object> root = new HashMap<>();
            root.put("testInstanceClass", getClassName(timeStepModel.getTestClass()));
            root.put("metronomeClass", getMetronomeClass(metronomeClass));
            root.put("timeStepMethods", timeStepModel.getActiveTimeStepMethods(executionGroup));
            root.put("probeClass", getClassName(probeClass));
            root.put("isStartNanos", new IsStartNanos(timeStepModel));
            root.put("isAssignableFrom", new IsAssignableFromMethod());
            root.put("isAsyncResult", new IsAsyncResult());
            root.put("Probe", LatencyProbe.class);
            root.put("threadStateClass", getClassName(timeStepModel.getThreadStateClass(executionGroup)));
            root.put("hasProbe", new HasProbeMethod());
            root.put("className", className);
            if (logFrequency > 0) {
                root.put("logFrequency", "" + logFrequency);
            }

            if (logRateMs > 0) {
                root.put("logRateMs", "" + logRateMs);
            }

            if (hasIterationCap) {
                root.put("hasIterationCap", "true");
            }

            Template temp = cfg.getTemplate("TimeStepLoop.ftl");
            StringWriter out = new StringWriter();
            temp.process(root, out);

            String javaCode = out.toString();
            File javaFile = new File(targetDirectory, className + ".java");

            writeText(javaCode, javaFile);

            return new JavaSourceFromString(className, javaCode);
        } catch (Exception e) {
            throw new IllegalTestException(className + " ran into a code generation problem: " + e.getMessage(), e);
        }
    }

    private static String getClassName(Class clazz) {
        return clazz == null ? null : clazz.getName().replace('$', '.');
    }

    private static String getMetronomeClass(Class<? extends Metronome> metronomeClass) {
        if (metronomeClass == EmptyMetronome.class) {
            return null;
        }
        return getClassName(metronomeClass);
    }

    private static class JavaSourceFromString extends SimpleJavaFileObject {

        private final String code;

        JavaSourceFromString(String name, String code) {
            super(URI.create("string:///" + name.replace('.', '/') + Kind.SOURCE.extension), Kind.SOURCE);
            this.code = code;
        }

        @Override
        public CharSequence getCharContent(boolean ignoreEncodingErrors) {
            return code;
        }
    }

    private static final class IsAssignableFromMethod implements TemplateMethodModelEx {

        @Override
        @SuppressWarnings("unchecked")
        public Object exec(List list) throws TemplateModelException {
            if (list.size() != 2) {
                throw new TemplateModelException("Wrong number of arguments for method isAssignableFrom()."
                        + " Method has two required parameters: [Class, Class]. Found: " + list.size());
            }

            Object arg1 = ((WrapperTemplateModel) list.get(0)).getWrappedObject();
            if (!(arg1 instanceof Class)) {
                throw new TemplateModelException("Wrong type of the first parameter."
                        + " It should be Class. Found: " + arg1.getClass());
            }

            Object arg2 = ((WrapperTemplateModel) list.get(1)).getWrappedObject();
            if (!(arg2 instanceof Class)) {
                throw new TemplateModelException("Wrong type of the second parameter."
                        + " It should be Class. Found: " + arg2.getClass());
            }

            return ((Class) arg2).isAssignableFrom((Class) arg1);
        }
    }

    private static final class IsAsyncResult implements TemplateMethodModelEx {
        @Override
        public Object exec(List list) throws TemplateModelException {
            if (list.size() != 1) {
                throw new TemplateModelException("Wrong number of arguments for method isAsyncResult()."
                        + " Method has one required parameter: [String]. Found: " + list.size());
            }

            String resultTypeName = ((SimpleScalar) list.get(0)).getAsString();
            return "java.util.concurrent.CompletableFuture".equals(resultTypeName);
        }
    }

    private static final class IsStartNanos implements TemplateMethodModelEx {

        private final TimeStepModel timeStepModel;

        public IsStartNanos(TimeStepModel timeStepModel) {
            this.timeStepModel = timeStepModel;
        }

        @Override
        @SuppressWarnings("unchecked")
        public Object exec(List list) throws TemplateModelException {
            if (list.size() != 2) {
                throw new TemplateModelException("Wrong number of arguments for method isAssignableFrom()."
                        + " Method has two required parameters: [Class, SimpleNumber]. Found: " + list.size());
            }

            Object arg1 = ((WrapperTemplateModel) list.get(0)).getWrappedObject();
            if (!(arg1 instanceof Method)) {
                throw new TemplateModelException("Wrong type of the first parameter."
                        + " It should be Method. Found: " + arg1.getClass());
            }

            Object arg2 = list.get(1);
            if (!(arg2 instanceof SimpleNumber)) {
                throw new TemplateModelException("Wrong type of the second parameter."
                        + " It should be SimpleNumber. Found: " + arg2.getClass());
            }

            return timeStepModel.hasStartNanosAnnotation((Method) arg1, ((SimpleNumber) arg2).getAsNumber().intValue() - 1);
        }
    }

    private static final class HasProbeMethod implements TemplateMethodModelEx {

        @Override
        public Object exec(List list) throws TemplateModelException {
            if (list.size() != 1) {
                throw new TemplateModelException("Wrong number of arguments for method hasProbe()."
                        + " Method has one required parameter: [Method]. Found: " + list.size());
            }

            Object arg1 = ((WrapperTemplateModel) list.get(0)).getWrappedObject();
            if (!(arg1 instanceof Method)) {
                throw new TemplateModelException("Wrong type of the first parameter."
                        + " It should be Method. Found: " + arg1.getClass());
            }

            Method method = (Method) arg1;
            for (Class paramType : method.getParameterTypes()) {
                if (LatencyProbe.class.isAssignableFrom(paramType)) {
                    return true;
                }
            }

            return false;
        }
    }
}
