package com.example.services;

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.util.Map;

import com.example.db.DatabaseManager;
import com.example.model.Node;

public class  HeartbeatReceiver {
    
    private static final int UDP_PORT = 9001;

    private final Map<String, Node> registeredNodes;
    private final DatabaseManager   db;

    public HeartbeatReceiver(Map<String, Node> registeredNodes, DatabaseManager db) {
        this.registeredNodes = registeredNodes;
        this.db = db;
    }

    public void start() {
        Thread t = new Thread(this::listen, "heartbeat-receiver");
        t.setDaemon(true);
        t.start();
    }

    private void listen() {
        try (DatagramSocket socket = new DatagramSocket(UDP_PORT)) {
            byte[] buffer = new byte[256];
            while (!Thread.currentThread().isInterrupted()) {
                DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
                socket.receive(packet);
                String msg = new String(packet.getData(), 0, packet.getLength()).trim();
                handle(msg, packet.getAddress().getHostAddress());
            }
        } catch (Exception e) {
            System.err.println("[HEARTBEAT] ERRO: " + e.getMessage());
        }
    }

    private void handle(String message, String fromIp) {
        if (!message.startsWith("HB:")) return;

        String nodeId = message.substring(3);
        Node node = registeredNodes.get(nodeId);
        if (node == null) return;

        db.updateHeartbeat(nodeId);

        if (!node.available) {
            node.available = true;
            db.updateHeartbeat(nodeId);
            System.out.println("[HEARTBEAT] Nó reativado: " + nodeId + " (" + fromIp + ")");
        }
    }

}
