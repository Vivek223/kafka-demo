package com.vivekt;

public class UserEvent {
    private String id;
    private String name;
    private String action;

    // Constructors
    public UserEvent() {}
    public UserEvent(String id, String name, String action) {
        this.id = id;
        this.name = name;
        this.action = action;
    }

    // Getters & Setters
    public String getId() { return id; }
    public void setId(String id) { this.id = id; }
    public String getName() { return name; }
    public void setName(String name) { this.name = name; }
    public String getAction() { return action; }
    public void setAction(String action) { this.action = action; }

    @Override
    public String toString() {
        return "UserEvent{" +
                "id='" + id + '\'' +
                ", name='" + name + '\'' +
                ", action='" + action + '\'' +
                '}';
    }
}
