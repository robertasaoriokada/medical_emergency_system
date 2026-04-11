package com.example.model;

import java.io.Serializable;
import java.sql.Timestamp;

public class Node implements Serializable {

    private static final long serialVersionUID = 1L;

    public String    id;
    String    name;
    public String    type;        // "AMBULANCE" | "UPA"
    public String    host;
    public int       port;
    Timestamp lastContact;
    public volatile boolean available = true;

    public int maxPriority; // 1..5

    public Node(String id, String name, String type, String host, int port) {
        this(id, name, type, host, port, 5);
    }

    public Node(String id, String name, String type, String host, int port, int maxPriority) {
        this.id   = id;
        this.name = name;
        this.type = type;
        this.host = host;
        this.port = port;
        this.maxPriority = maxPriority;
    }

    // Getters — necessários fora do package
    public String    getId()          { return id; }
    public String    getName()        { return name; }
    public String    getType()        { return type; }
    public String    getHost()        { return host; }
    public int       getPort()        { return port; }
    public int       getMaxPriority() { return maxPriority; }
    public Timestamp getLastContact() { return lastContact; }
    public boolean   isAvailable()    { return available; }

    public void setAvailable(boolean available)    { this.available    = available; }
    public void setLastContact(Timestamp ts)       { this.lastContact  = ts; }
    public void setMaxPriority(int maxPriority)  { this.maxPriority = maxPriority; }

        
    public boolean canHandle(int priority) {
        return available && priority >= maxPriority;
    }
    
    @Override
    public String toString() {
        return String.format("Node[%s | %s | %s:%d | %s]",
                id, type, host, port, maxPriority, available ? "ONLINE" : "OFFLINE");
    }
}