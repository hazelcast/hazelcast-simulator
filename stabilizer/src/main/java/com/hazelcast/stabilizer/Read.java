package com.hazelcast.stabilizer;

import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;

public class Read {

    public static void main(String[] args) throws Exception {
        File file = new File("delete.out");
//        file.delete();
//        file.createNewFile();
//
//
//        final ObjectOutputStream out = new ObjectOutputStream(fis);
//        out.close();
//
//        ObjectInputStream in = new ObjectInputStream(new FileInputStream(file));
//
//        for (; ; ) {
//            try {
//                FileLock lock = channel.lock();
//                try {
//                    Object x = in.readObject();
//                    System.out.println(x);
//                }finally {
//                    lock.release();
//                }
//            } catch (EOFException e) {
//                Thread.sleep(1);
//            }
//        }
    }
}
