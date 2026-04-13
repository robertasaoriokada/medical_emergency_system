package com.example.services;

import java.net.*;

public class HeartbeatSender {

    private static final int INTERVAL_MS = 5_000; // envia a cada 5 segundos

    private final String nodeId;
    private final String centralHost;
    private final int    centralPort;

    private volatile boolean running = true;

    public HeartbeatSender(String nodeId, String centralHost, int centralPort) {
        this.nodeId      = nodeId;
        this.centralHost = centralHost;
        this.centralPort = centralPort;
    }

    public void start() {
        Thread hb = new Thread(this::loop, nodeId + "-heartbeat");
        hb.setDaemon(true);
        hb.start();
        log("Heartbeat UDP iniciado → " + centralHost + ":" + centralPort);
    }

    public void stop() {
        running = false;
    }

    // ---------------------------------------------------------------
    // Loop principal — mantém o DatagramSocket aberto e reutilizado
    // ---------------------------------------------------------------
    private void loop() {
        try (DatagramSocket udpSocket = new DatagramSocket()) {
            while (running && !Thread.currentThread().isInterrupted()) {
                send(udpSocket);
                Thread.sleep(INTERVAL_MS);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            log("ERRO no loop de heartbeat: " + e.getMessage());
        }
    }

    // ---------------------------------------------------------------
    // Envio de um único pacote UDP
    // ---------------------------------------------------------------
    private void send(DatagramSocket udpSocket) {
        try {
            byte[]         data   = ("HB:" + nodeId).getBytes();
            InetAddress    addr   = InetAddress.getByName(centralHost);
            DatagramPacket packet = new DatagramPacket(data, data.length, addr, centralPort);
            udpSocket.send(packet);
            log("Heartbeat enviado");
        } catch (Exception e) {
            log("ERRO ao enviar heartbeat: " + e.getMessage());
        }
    }

    private void log(String msg) {
        System.out.printf("[HB %-8s] %s%n", nodeId, msg);
    }
}