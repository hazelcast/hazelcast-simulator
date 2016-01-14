package com.hazelcast.simulator.utils.jars;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;

public class GitRepositoryTest {

    @Test
    public void testFromString() {
        GitRepository repo = GitRepository.fromString("hazelcast=https://github.com/hazelcast/hazelcast-simulator.git");

        assertNotNull(repo);
        assertEquals("hazelcast", repo.getName());
        assertEquals("https://github.com/hazelcast/hazelcast-simulator.git", repo.getUrl());
        assertNotNull(repo.toString());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testFromString_shouldThrowOnNull() {
        GitRepository.fromString(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testFromString_shouldThrowOnInvalidRepositoryString() {
        GitRepository.fromString("hazelcast/hazelcast-simulator");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testFromString_shouldThrowOnEmptyName() {
        GitRepository.fromString("=https://github.com/hazelcast/hazelcast-simulator.git");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testFromString_shouldThrowOnEmptyURL() {
        GitRepository.fromString("hazelcast= ");
    }

    @Test
    public void testEqualsAndHashCode() {
        GitRepository repo = GitRepository.fromString("hazelcast=https://github.com/hazelcast/hazelcast-simulator.git");
        GitRepository sameRepo = GitRepository.fromString("hazelcast=https://github.com/hazelcast/hazelcast-simulator.git");
        GitRepository otherRepoName = GitRepository.fromString("donnerbart=https://github.com/hazelcast/hazelcast-simulator.git");
        GitRepository otherRepoURL = GitRepository.fromString("hazelcast=https://github.com/hazelcast/hazelcast.git");

        assertEquals(repo, repo);
        assertEquals(repo, sameRepo);

        assertNotEquals(repo, null);
        assertNotEquals(repo, otherRepoName);
        assertNotEquals(repo, otherRepoURL);

        assertEquals(repo.hashCode(), repo.hashCode());
        assertEquals(repo.hashCode(), sameRepo.hashCode());

        assertNotEquals(repo.hashCode(), otherRepoName.hashCode());
        assertNotEquals(repo.hashCode(), otherRepoURL.hashCode());
    }
}
