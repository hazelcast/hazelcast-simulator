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
package com.hazelcast.simulator.utils.helper;

import java.security.Permission;

public final class ExitExceptionSecurityManager extends SecurityManager {

    private final boolean throwExceptionOnStatusZero;

    public ExitExceptionSecurityManager() {
        this.throwExceptionOnStatusZero = false;
    }

    public ExitExceptionSecurityManager(boolean throwExceptionOnStatusZero) {
        this.throwExceptionOnStatusZero = throwExceptionOnStatusZero;
    }

    @Override
    public void checkPermission(Permission perm) {
        // allow anything
    }

    @Override
    public void checkPermission(Permission perm, Object context) {
        // allow anything
    }

    @Override
    public void checkExit(int status) {
        super.checkExit(status);
        if (status == 0) {
            if (throwExceptionOnStatusZero) {
                throw new ExitStatusZeroException();
            }
            return;
        }
        if (status == 1) {
            throw new ExitStatusOneException();
        }
        throw new ExitException(status);
    }
}
