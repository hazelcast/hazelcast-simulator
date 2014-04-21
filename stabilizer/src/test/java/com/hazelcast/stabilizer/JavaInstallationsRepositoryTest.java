package com.hazelcast.stabilizer;

import com.hazelcast.stabilizer.agent.JavaInstallation;
import com.hazelcast.stabilizer.agent.JavaInstallationsRepository;
import org.junit.Test;

import java.io.File;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class JavaInstallationsRepositoryTest {

    @Test
    public void test() throws Exception {
        JavaInstallationsRepository repository = new JavaInstallationsRepository();
        File file = TestUtils.writeToTempFile("" +
                        "1.vendor=sun\n" +
                        "1.version=1.5\n" +
                        "1.javaHome=/tmp\n" +
                        "2.vendor=sun\n" +
                        "2.version=1.6\n" +
                        "2.javaHome=/tmp\n" +
                        "3.vendor=openjdk\n" +
                        "3.version=1.5\n" +
                        "3.javaHome=/tmp\n"
        );
        repository.load(file);

        JavaInstallation installation = repository.get("sun", "1.5");
        assertNotNull(installation);
        assertEquals("sun", installation.getVendor());
        assertEquals("1.5", installation.getVersion());
        assertEquals("/tmp", installation.getJavaHome());

        installation = repository.get("openjdk", "1.5");
        assertNotNull(installation);
        assertEquals("openjdk", installation.getVendor());
        assertEquals("1.5", installation.getVersion());
        assertEquals("/tmp", installation.getJavaHome());

        installation = repository.get("openjdk", "1.0");
        assertNull(installation);
    }
}
