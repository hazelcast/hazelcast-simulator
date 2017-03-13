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

import com.hazelcast.simulator.coordinator.registry.AgentData;
import com.hazelcast.simulator.coordinator.registry.ComponentRegistry;
import com.hazelcast.simulator.utils.TagUtils;
import freemarker.template.Configuration;
import freemarker.template.SimpleScalar;
import freemarker.template.Template;
import freemarker.template.TemplateExceptionHandler;
import freemarker.template.TemplateMethodModelEx;
import freemarker.template.TemplateModelException;

import java.io.StringReader;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.hazelcast.simulator.utils.Preconditions.checkNotNull;

public class ConfigFileTemplate {

    private final String rawTemplate;
    private final Map<String, String> hardReplacements = new HashMap<String, String>();
    private final Map<String, Object> environment = new HashMap<String, Object>();
    private ComponentRegistry componentRegistry;

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

    public ConfigFileTemplate withComponentRegistry(ComponentRegistry componentRegistry) {
        this.componentRegistry = componentRegistry;
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

            if (componentRegistry != null) {
                root.put("agents", new Agents());
            }
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

    private final class Agents implements TemplateMethodModelEx {

        @Override
        public Object exec(List list) throws TemplateModelException {
            if (list.size() == 0) {
                return componentRegistry.getAgents();
            } else if (list.size() == 1) {
                Object arg = list.get(0);
                if (!(arg instanceof SimpleScalar)) {
                    throw new TemplateModelException("Wrong type of the first parameter."
                            + " It should be SimpleScalar . Found: " + arg.getClass());
                }

                Map<String, String> tags = TagUtils.parseTags(((SimpleScalar) arg).getAsString());
                List<AgentData> result = new ArrayList<AgentData>();
                for (AgentData agent : componentRegistry.getAgents()) {
                    if (TagUtils.matches(tags, agent.getTags())) {
                        result.add(agent);
                    }
                }
                return result;
            } else {
                throw new TemplateModelException("Wrong number of arguments for method agents()."
                        + " Method has zero a 1 String argument, found " + list.size());
            }
        }
    }
}
