package com.hazelcast.simulator.provisioner.git;

public class GitRepository {

    private final String name;
    private final String url;

    public GitRepository(String name, String url) {
        this.name = name;
        this.url = url;
    }

    public String getName() {
        return name;
    }

    public String getUrl() {
        return url;
    }

    public static GitRepository fromString(String repositoryString) {
        if (repositoryString == null || repositoryString.isEmpty()) {
            throw new IllegalArgumentException("Repository String cannot be empty.");
        }
        String[] repoSplit = repositoryString.split("=");
        if (repoSplit.length != 2) {
            throw new IllegalArgumentException("Invalid Repository " + repositoryString
                    + "\nRepository has to be in this format: <name>=<url>, e.g.\n"
                    + "jaromir=https://github.com/hazelcast/hazelcast-simulator.git");
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
