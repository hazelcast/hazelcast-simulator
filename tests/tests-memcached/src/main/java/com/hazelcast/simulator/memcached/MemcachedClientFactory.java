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
package com.hazelcast.simulator.memcached;

import net.spy.memcached.MemcachedClient;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

public class MemcachedClientFactory {

    protected final Logger logger = Logger.getLogger(getClass());

    private List<MemcachedClient> clients = new ArrayList<MemcachedClient>();
    private List<InetSocketAddress> addresses = new ArrayList<InetSocketAddress>();
    private boolean useSharedClient;

    public MemcachedClientFactory(List<InetSocketAddress> addresses) {
        this.addresses = addresses;
    }

    public synchronized MemcachedClient create() {
        if (useSharedClient && clients.size() == 1) {
            logger.info("Using a shared client");
            return clients.get(0);
        }

        logger.info("Creating a new client");

        MemcachedClient client = null;
        try {
            client = new MemcachedClient(this.addresses);
        } catch (IOException ex) {
            throw new IllegalStateException("Error starting Memcached client.", ex);
        }

        clients.add(client);
        return client;
    }

    public void shutdown() {
        for (MemcachedClient client: clients) {
            client.shutdown();
        }
    }

    public void setUseSharedClient(boolean useSharedClient) {
        this.useSharedClient = useSharedClient;
    }
}
