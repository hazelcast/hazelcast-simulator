package com.hazelcast.simulator.utils.helper;

import java.security.Permission;

public final class ExitExceptionSecurityManager extends SecurityManager {

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
            return;
        }
        if (status == 1) {
            throw new ExitStatusOneException();
        }
        throw new ExitException(status);
    }
}
