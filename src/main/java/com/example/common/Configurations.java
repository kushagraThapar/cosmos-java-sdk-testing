package com.example.common;

public class Configurations {

    public final static String endpoint = System.getProperty("endpoint", System.getenv("endpoint"));
    public final static String key = System.getProperty("key", System.getenv("key"));
}
