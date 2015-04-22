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
                throw new ExitException(0);
            }
            return;
        }
        if (status == 1) {
            throw new ExitStatusOneException();
        }
        throw new ExitException(status);
    }
}
