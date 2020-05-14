package com.xh.flink.pojo;

public class Foo{
    private Integer id;
    private String name;
    private String other;

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getOther() {
        return other;
    }

    public void setOther(String other) {
        this.other = other;
    }

    @Override
    public String toString() {
        return "foo{" +
                "id=" + id +
                ", name='" + name + '\'' +
                ", other='" + other + '\'' +
                '}';
    }
}