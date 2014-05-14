package com.hazelcast.stabilizer.tests;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;

public class TestMethods {

    public Method runMethod;

    public Method setupMethod;

    public TestMethods(Class clazz){
        this.clazz = clazz
        clazz.getMethods()
    }


    private Method findMethod(Annotation annotation){

    }
 }
