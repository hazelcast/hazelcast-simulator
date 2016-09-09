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

import com.hazelcast.simulator.utils.FileUtils;
import freemarker.template.Configuration;
import freemarker.template.Template;
import freemarker.template.TemplateExceptionHandler;

import java.io.File;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.Map;

import static com.hazelcast.simulator.utils.Preconditions.checkNotNull;

public class ConfigFileTemplate {

    private final String rawTemplate;
    private final Map<String, String> hardReplacements = new HashMap<String, String>();
    private final Map<String, Object> environment = new HashMap<String, Object>();

    public ConfigFileTemplate(File file) {
        this(FileUtils.fileAsText(file));
    }

    public ConfigFileTemplate(String rawTemplate) {
        this.rawTemplate = checkNotNull(rawTemplate, "rawTemplate can't be null");
    }

    public ConfigFileTemplate addEnvironment(String key, Object value) {
        this.environment.put(key, value);
        return this;
    }

    public ConfigFileTemplate addEnvironment(Map<String, ?> env) {
        this.environment.putAll(env);
        return this;
    }

    public ConfigFileTemplate addReplacement(String key, String value) {
        this.hardReplacements.put(key, value);
        return this;
    }

    public String render() {
        try {
            Configuration cfg = new Configuration(Configuration.VERSION_2_3_24);
            cfg.setDefaultEncoding("UTF-8");
            cfg.setTemplateExceptionHandler(TemplateExceptionHandler.RETHROW_HANDLER);
            cfg.setLogTemplateExceptions(false);

            Map<String, Object> root = new HashMap<String, Object>();
            root.putAll(environment);

            String templateStr = loadTemplateString();
            Template template = new Template("name", new StringReader(templateStr), cfg);
            StringWriter out = new StringWriter();
            template.process(root, out);
            return out.toString();
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private String loadTemplateString() {
        String s = rawTemplate;

        for (Map.Entry<String, String> entry : hardReplacements.entrySet()) {
            s = s.replace(entry.getKey(), entry.getValue());

        }

        return s;
    }
}
