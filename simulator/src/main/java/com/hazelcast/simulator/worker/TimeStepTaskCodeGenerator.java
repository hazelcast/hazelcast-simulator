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

package com.hazelcast.simulator.worker;

import com.hazelcast.simulator.probes.Probe;
import com.hazelcast.simulator.probes.impl.HdrProbe;
import com.hazelcast.simulator.test.IllegalTestException;
import com.hazelcast.simulator.utils.FileUtils;
import com.hazelcast.simulator.worker.metronome.BusySpinningMetronome;
import com.hazelcast.simulator.worker.metronome.Metronome;
import freemarker.ext.util.WrapperTemplateModel;
import freemarker.template.Configuration;
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
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * todo:
 * - determining the right probe class
 * - the timestep methods now all get equal chance to run. Which is not valid
 * - loading the right probe instance per timestep method.
 * - what should happen when no probe-class is defined, but the user has a probe method.
 * - detecting that timestep method only has probe/context args
 * - rename worker
 * - isWorkerStopped
 * - better id for the timesteptask. Needs to be the same on all members.
 * <p>
 * <p>
 * <p>
 * done:
 * - location of the timesteptask class file
 * - timesteptask needs to have unique name.
 * - class file should be written of the test
 * - compile error of generated code exception
 * - property binding merge into DependencyInjector.
 * - keep track of property usage. If property not used, then throw an error.
 * - after completion & after run?
 * - split class hierarchies (no need to use IWorker) for the new worker
 * - configure interval in us for metronome
 * - single mechanism for determining the metronome
 * - timestep methods can handle exception
 * - generating 2 arg method method
 * - each timestep method has unique name
 * - detecting that each timestep method has unique name
 * - ensure all before/after/timestep methods are public
 * - ensure that beforeAfterRun have at most 1 argument
 * - ensure that before/afterRun methods have no probe argument
 * - probe defined per timestep method
 * - in case of calling with probe argument, then no time tracking for probe should be done
 * - determining the right metronome class
 */
public class TimeStepTaskCodeGenerator {

    private static final AtomicLong ID_GENERATOR = new AtomicLong();

    private final JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();

    public Class compile(
            TimeStepModel timeStepModel,
            Class<? extends Metronome> metronomeClass,
            Class<? extends Probe> probeClass) {

        String id = "" + ID_GENERATOR.incrementAndGet();
        JavaFileObject file = createJavaFileObject(id, metronomeClass, timeStepModel, probeClass);
        return compile(file, id);
    }

    public static void main(String[] args) throws Exception {
        TimeStepTaskCodeGenerator codeGenerator = new TimeStepTaskCodeGenerator();
        DummyTest testInstance = new DummyTest();
        TimeStepModel timeStepModel = new TimeStepModel(testInstance.getClass());
        Class clazz = codeGenerator.compile(timeStepModel, BusySpinningMetronome.class, HdrProbe.class);
    }

    private Class compile(JavaFileObject file, String testId) {
        DiagnosticCollector<JavaFileObject> diagnostics = new DiagnosticCollector<JavaFileObject>();

        JavaCompiler.CompilationTask task = compiler.getTask(
                null,
                null,
                diagnostics,
                null,
                null,
                Arrays.asList(file));

        boolean success = task.call();

        if (!success) {
            StringBuilder sb = new StringBuilder();
            for (Diagnostic diagnostic : diagnostics.getDiagnostics()) {
                sb.append("Error on line")
                        .append(diagnostic.getLineNumber())
                        .append(" in ")
                        .append(diagnostic)
                        .append('\n');
            }
            throw new IllegalTestException(sb.toString());
        }

        try {
            URLClassLoader classLoader = new URLClassLoader(new URL[]{new File("./").toURI().toURL()});
            return (Class) classLoader.loadClass("GeneratedTimeStepTask_" + testId);
        } catch (ClassNotFoundException e) {
            throw new IllegalTestException(e.getMessage(), e);
        } catch (MalformedURLException e) {
            throw new IllegalTestException(e.getMessage(), e);
        }
    }

    private JavaFileObject createJavaFileObject(
            String testId,
            Class<? extends Metronome> metronomeClass,
            TimeStepModel timeStepModel,
            Class<? extends Probe> probeClass) {

        try {
            Configuration cfg = new Configuration(Configuration.VERSION_2_3_24);
            cfg.setClassForTemplateLoading(this.getClass(), "/");
            cfg.setDefaultEncoding("UTF-8");
            cfg.setTemplateExceptionHandler(TemplateExceptionHandler.RETHROW_HANDLER);
            cfg.setLogTemplateExceptions(false);

            Map<String, Object> root = new HashMap<String, Object>();
            root.put("testInstanceClass", getClassName(timeStepModel.getTestClass()));
            root.put("metronomeClass", getClassName(metronomeClass));
            root.put("timeStepMethods", timeStepModel.getTimeStepMethods());
            root.put("probeClass", getClassName(probeClass));
            root.put("isAssignableFrom", new IsAssignableFromMethod());
            root.put("Probe", Probe.class);
            root.put("threadContextClass", getClassName(timeStepModel.getThreadContextClass()));
            root.put("hasProbe", new HasProbeMethod());
            root.put("id", testId);

            Template temp = cfg.getTemplate("generatedtimesteptask.ftl");
            StringWriter out = new StringWriter();
            temp.process(root, out);

            String javaCode = out.toString();

            File javaFile = new File("GeneratedTimeStepTask_" + testId + ".java");
            FileUtils.writeText(javaCode, javaFile);

            return new JavaSourceFromString("GeneratedTimeStepTask_" + testId, javaCode);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private String getClassName(Class clazz) {
        return clazz == null ? null : clazz.getName().replace('$', '.');
    }

    static class JavaSourceFromString extends SimpleJavaFileObject {
        final String code;

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
        public Object exec(List list) throws TemplateModelException {
            if (list.size() != 2) {
                throw new TemplateModelException("Wrong arguments for method 'isAssignableFrom'. "
                        + "Method has two required parameters: object and class");
            }

            Object arg1 = ((WrapperTemplateModel) list.get(0)).getWrappedObject();
            if (!(arg1 instanceof Class)) {
                throw new TemplateModelException("Wrong type of the first parameter. "
                        + "It should be Class. Found: " + arg1.getClass());
            }

            Object arg2 = ((WrapperTemplateModel) list.get(1)).getWrappedObject();
            if (!(arg2 instanceof Class)) {
                throw new TemplateModelException("Wrong type of the second parameter. "
                        + "It should be Class. Found: " + arg2.getClass());
            }

            Class c = (Class) arg2;
            return c.isAssignableFrom((Class) arg1);
        }
    }

    private static final class HasProbeMethod implements TemplateMethodModelEx {

        @Override
        public Object exec(List list) throws TemplateModelException {
            if (list.size() != 1) {
                throw new TemplateModelException("Wrong arguments for method 'hasProbe'. "
                        + "Method has 1 required parameters: Method");
            }

            Object arg1 = ((WrapperTemplateModel) list.get(0)).getWrappedObject();
            if (!(arg1 instanceof Method)) {
                throw new TemplateModelException("Wrong type of the first parameter. "
                        + "It should be Method. Found: " + arg1.getClass());
            }

            Method method = (Method) arg1;
            for (Class paramType : method.getParameterTypes()) {
                if (Probe.class.isAssignableFrom(paramType)) {
                    return true;
                }
            }

            return false;
        }
    }
}


