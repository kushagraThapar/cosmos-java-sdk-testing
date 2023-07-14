package com.example.common;

public class TestItem {

    String id;
    String pk;
    public TestItem(String id, String pk) {
        this.id = id;
        this.pk = pk;
    }

    public TestItem() {
    }

    public String getId() {
        return id;
    }

    public String getPk() {
        return pk;
    }
}
