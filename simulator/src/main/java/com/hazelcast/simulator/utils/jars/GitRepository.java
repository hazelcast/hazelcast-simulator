/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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
package com.hazelcast.simulator.utils.jars;

import static com.hazelcast.simulator.utils.FormatUtils.NEW_LINE;

class GitRepository {

    private final String name;
    private final String url;

    GitRepository(String name, String url) {
        this.name = name;
        this.url = url;
    }

    String getName() {
        return name;
    }

    String getUrl() {
        return url;
    }

    static GitRepository fromString(String repositoryString) {
        if (repositoryString == null || repositoryString.isEmpty()) {
            throw new IllegalArgumentException("Repository String cannot be empty.");
        }
        String[] repoSplit = repositoryString.split("=");
        if (repoSplit.length != 2) {
            throw new IllegalArgumentException("Invalid Repository " + repositoryString
                    + NEW_LINE + "Repository has to be in this format: <name>=<url>, e.g."
                    + NEW_LINE + "jaromir=https://github.com/hazelcast/hazelcast-simulator.git");
        }
        String name = repoSplit[0].trim();
        if (name.isEmpty()) {
            throw new IllegalArgumentException("Repository '" + repositoryString + "' has empty name.");
        }
        String url = repoSplit[1].trim();
        if (url.isEmpty()) {
            throw new IllegalArgumentException("Repository '" + repositoryString + "' has URL name.");
        }
        return new GitRepository(name, url);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        GitRepository that = (GitRepository) o;

        if (name != null ? !name.equals(that.name) : that.name != null) {
            return false;
        }
        if (url != null ? !url.equals(that.url) : that.url != null) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = name != null ? name.hashCode() : 0;
        result = 31 * result + (url != null ? url.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "GitRepository{"
                + "name='" + name + '\''
                + ", url='" + url + '\''
                + '}';
    }
}
