package com.hazelcast.stabilizer;

import java.io.File;

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
