package com.g414.inno.db;


public class Table {
    private final String name;
    private final Long id;

    public Table(String name, Long id) {
        this.name = name;
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public Long getId() {
        return id;
    }
}
