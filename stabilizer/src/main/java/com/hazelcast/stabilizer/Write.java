package com.hazelcast.stabilizer;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.nio.channels.FileLock;

/**
 * Created by alarmnummer on 4/21/14.
 */
public class Write {

    public static void main(String[] args) throws Exception {
        File workerInputFile = new File("delete.out");

        final FileOutputStream out1 = new FileOutputStream(workerInputFile);
        final ObjectOutputStream out = new ObjectOutputStream(out1);


        new Thread() {
            public void run() {
                int k = 0;
                for (; ; ) {
                    Utils.sleepMillis(100);
                    try {
                        FileLock lock = out1.getChannel().lock();
                        try {
                            out.writeObject(k);
                            out.flush();
                        }finally{
                            lock.release();
                        }
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    k++;
                }
            }
        }.start();
    }
}
