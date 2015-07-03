package com.hazelcast.simulator.tests.icache.helpers;

import java.io.Serializable;

public class ICacheCreateDestroyCounter implements Serializable {

    public long put;
    public long create;
    public long close;
    public long destroy;

    public long putException;
    public long createException;
    public long closeException;
    public long destroyException;

    public void add(ICacheCreateDestroyCounter c) {
        put += c.put;
        create += c.create;
        close += c.close;
        destroy += c.destroy;

        putException += c.putException;
        createException += c.createException;
        closeException += c.closeException;
        destroyException += c.destroyException;
    }

    @Override
    public String toString() {
        return "Counter{"
                + "put=" + put
                + ", create=" + create
                + ", close=" + close
                + ", destroy=" + destroy
                + ", putException=" + putException
                + ", createException=" + createException
                + ", closeException=" + closeException
                + ", destroyException=" + destroyException
                + '}';
    }
}
