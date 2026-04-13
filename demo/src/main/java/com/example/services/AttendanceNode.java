package com.example.services;

import com.example.db.DatabaseManager;
import com.example.model.Occurrence;
import com.example.model.Occurrence.Status;

import java.io.*;
import java.net.*;
import java.sql.Timestamp;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class AttendanceNode {

    private static final int    THREAD_POOL  = 5;
    private static final String CENTRAL_HOST = "localhost";

    private final String          nodeId;
    private final String          name;
    private final String          type;
    private final int             port;
    private final DatabaseManager db;
    private final HeartbeatSender heartbeatSender;

    private volatile boolean      running = true;
    private final ExecutorService threadPool;

    public AttendanceNode(String nodeId, String name, String type, int port) {
        this.nodeId          = nodeId;
        this.name            = name;
        this.type            = type;
        this.port            = port;
        this.db              = new DatabaseManager();
        this.heartbeatSender = new HeartbeatSender(nodeId, CENTRAL_HOST, 9001);
        this.threadPool      = Executors.newFixedThreadPool(THREAD_POOL, r -> {
            Thread t = new Thread(r, nodeId + "-worker-" + System.nanoTime());
            t.setDaemon(true);
            return t;
        });
    }

    // ---------------------------------------------------------------
    // Inicialização
    // ---------------------------------------------------------------
    public void start() {
        heartbeatSender.start();
        listenForDispatches();
    }

    // ---------------------------------------------------------------
    // Servidor TCP — recebe despachos do servidor central
    // ---------------------------------------------------------------
    private void listenForDispatches() {
        try (ServerSocket server = new ServerSocket(port)) {
            server.setReuseAddress(true);
            log("Escutando despachos na porta " + port);

            while (running) {
                Socket connection = server.accept();
                threadPool.submit(() -> handleDispatch(connection));
            }
        } catch (IOException e) {
            log("ERRO no servidor de despachos: " + e.getMessage());
        }
    }

    // ---------------------------------------------------------------
    // Tratamento de cada despacho
    // ---------------------------------------------------------------
    private void handleDispatch(Socket socket) {
        String from = socket.getInetAddress().getHostAddress();
        log("Despacho recebido de: " + from);

        try (
            ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
            ObjectInputStream  in  = new ObjectInputStream(socket.getInputStream())
        ) {
            out.flush();

            Object obj = in.readObject();

            if (!(obj instanceof Occurrence occurrence)) {
                log("Objeto inválido recebido — ignorando");
                out.writeObject("ERR:objeto inválido");
                return;
            }

            log("Ocorrência recebida: " + occurrence);
            processOccurrence(occurrence);

            String ack = "ACK:" + occurrence.getId();
            out.writeObject(ack);
            out.flush();
            log("ACK enviado → " + occurrence.getId().substring(0, 8));

        } catch (Exception e) {
            log("ERRO ao processar despacho: " + e.getMessage());
        } finally {
            try { socket.close(); } catch (IOException ignored) {}
        }
    }

    // ---------------------------------------------------------------
    // Lógica de atendimento
    // ---------------------------------------------------------------
    private void processOccurrence(Occurrence occurrence) {
        log("Processando ocorrência " + occurrence.getId().substring(0, 8)
                + " | tipo=" + occurrence.getType()
                + " | prioridade=" + occurrence.getPriority()
                + " | cor=" + occurrence.getColor());

        long simulatedMs = (long) occurrence.getPriority() * 100L;
        try {
            Thread.sleep(simulatedMs);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        occurrence.setStatus(Status.COMPLETED);
        Timestamp completedAt = new Timestamp(System.currentTimeMillis());
        db.updateOccurrenceCompleted(occurrence.getId(), completedAt);

        log("Atendimento concluído: " + occurrence.getId().substring(0, 8)
                + " em ~" + simulatedMs + "ms simulados"
                + " | completedAt=" + completedAt);
    }

    // ---------------------------------------------------------------
    // Encerramento gracioso
    // ---------------------------------------------------------------
    public void stop() {
        running = false;
        heartbeatSender.stop();
        threadPool.shutdownNow();
        db.close();
        log("Nó encerrado.");
    }

    private void log(String msg) {
        System.out.printf("[NÓ %-8s %s] %s%n",
                nodeId, new Timestamp(System.currentTimeMillis()), msg);
    }

    // ---------------------------------------------------------------
    // Main — inicia os três nós em threads separadas
    // ---------------------------------------------------------------
    public static void main(String[] args) {
        AttendanceNode samu1  = new AttendanceNode("SAMU_1",  "Ambulância SAMU 1", "AMBULANCE", 9100);
        AttendanceNode upaSul = new AttendanceNode("UPA_SUL", "UPA Zona Sul",       "UPA",       9101);
        AttendanceNode samu2  = new AttendanceNode("SAMU_2",  "Ambulância SAMU 2",  "AMBULANCE", 9102);

        new Thread(samu1::start,  "thread-SAMU_1").start();
        new Thread(upaSul::start, "thread-UPA_SUL").start();
        new Thread(samu2::start,  "thread-SAMU_2").start();
    }
}