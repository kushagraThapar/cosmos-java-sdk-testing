package com.example.common;

public class Pojo {

    String id;
    String pk;
    String field;

    public Pojo(String id, String pk, String field) {
        this.id = id;
        this.pk = pk;
        this.field = field;
    }

    public Pojo() {
    }

    public String getId() {
        return id;
    }

    public String getPk() {
        return pk;
    }

    public String getField() {
        return field;
    }

    @Override
    public String toString() {
        return "Pojo{" +
                "id='" + id + '\'' +
                ", pk='" + pk + '\'' +
                ", field='" + field + '\'' +
                '}';
    }
}
