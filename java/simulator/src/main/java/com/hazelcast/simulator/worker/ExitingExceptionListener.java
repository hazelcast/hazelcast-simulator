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

import com.hazelcast.simulator.utils.ExceptionReporter;

import javax.jms.ExceptionListener;
import javax.jms.JMSException;

/**
 * An ExceptionListener that reports the exception (so written to disk) and then just exits the JVM. In case of a worker
 * having connection problems to the broker, it is best to crash.
 */
public class ExitingExceptionListener implements ExceptionListener {
    @Override
    public void onException(JMSException e) {
        ExceptionReporter.report(null, e);
        System.exit(1);
    }
}
