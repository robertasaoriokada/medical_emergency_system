package com.example.services;

import com.example.model.Occurrence;
import com.example.model.Occurrence.Status;

import java.io.*;
import java.net.*;
import java.sql.Timestamp;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Nó de atendimento (Camada 3 do diagrama).
 * Escuta em sua própria porta TCP, recebe Occurrence do servidor central,
 * processa e devolve ACK de confirmação.
 *
 * Uso:
 *   new AttendanceNode("SAMU_1", "Ambulância SAMU 1", "AMBULANCE", 9100).start();
 *   new AttendanceNode("UPA_SUL", "UPA Zona Sul",      "UPA",       9101).start();
 *   new AttendanceNode("SAMU_2", "Ambulância SAMU 2", "AMBULANCE", 9102).start();
 */
public class AttendanceNode {

    private static final int    THREAD_POOL     = 5;
    private static final String CENTRAL_HOST    = "localhost";
    private static final int    CENTRAL_PORT    = 9000;
    private static final int    CONNECT_TIMEOUT = 5_000;
    private static final int    READ_TIMEOUT    = 10_000;

    private final String nodeId;
    private final String name;
    private final String type;       // "AMBULANCE" | "UPA"
    private final int    port;

    private volatile boolean running = true;

    private final ExecutorService threadPool;
    private final HeartbeatSender heartbeatSender;


    public AttendanceNode(String nodeId, String name, String type, int port) {
        this.nodeId     = nodeId;
        this.name       = name;
        this.type       = type;
        this.port       = port;
        this.threadPool = Executors.newFixedThreadPool(THREAD_POOL, r -> {
            Thread t = new Thread(r, nodeId + "-worker-" + System.nanoTime());
            t.setDaemon(true);
            return t;
        });
        this.heartbeatSender = new HeartbeatSender(nodeId, CENTRAL_HOST, 9001);

    }

    // ---------------------------------------------------------------
    // Inicialização
    // ---------------------------------------------------------------
    public void start() {
        registerWithCentral();
        heartbeatSender.start();
        listenForDispatches();
    }

    // ---------------------------------------------------------------
    // Registro no servidor central
    // ---------------------------------------------------------------
    private void registerWithCentral() {
        // O servidor já pré-registra os nós em TCPService.main(),
        // mas em uma arquitetura real o nó se auto-registraria via
        // mensagem de controle. Logamos apenas para visibilidade.
        log("Nó inicializado — aguardando despachos na porta " + port);
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
    // Tratamento de cada despacho recebido
    // ---------------------------------------------------------------
    private void handleDispatch(Socket socket) {
        String from = socket.getInetAddress().getHostAddress();
        log("Despacho recebido de: " + from);

        try (
            ObjectInputStream  in  = new ObjectInputStream(socket.getInputStream());
            ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream())
        ) {
            Object obj = in.readObject();

            if (!(obj instanceof Occurrence occurrence)) {
                log("Objeto inválido recebido — ignorando");
                out.writeObject("ERR:objeto inválido");
                return;
            }

            log("Ocorrência recebida: " + occurrence);

            // Simula tempo de processamento do atendimento
            processOccurrence(occurrence);

            // Envia ACK de confirmação ao servidor central
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
    // Lógica de atendimento (simulada)
    // Aqui entraria integração com sistemas reais do nó
    // ---------------------------------------------------------------
    private void processOccurrence(Occurrence occurrence) {
        log("Processando ocorrência " + occurrence.getId().substring(0, 8)
                + " | tipo=" + occurrence.getType()
                + " | prioridade=" + occurrence.getPriority()
                + " | cor=" + occurrence.getColor());

        // Simula tempo proporcional à prioridade
        // Prioridade 1 (crítico) → atendimento imediato (100ms simulado)
        // Prioridade 5 (não urgente) → 500ms simulado
        long simulatedMs = (long) occurrence.getPriority() * 100L;
        try {
            Thread.sleep(simulatedMs);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        occurrence.setStatus(Status.COMPLETED);
        log("Atendimento concluído: " + occurrence.getId().substring(0, 8)
                + " em ~" + simulatedMs + "ms simulados");
    }

    // ---------------------------------------------------------------
    // Encerramento gracioso
    // ---------------------------------------------------------------
    public void stop() {
        running = false;
        heartbeatSender.stop(); // para o envio de heartbeats junto com o nó
        threadPool.shutdownNow();
        log("Nó encerrado.");
    }

    // ---------------------------------------------------------------
    // Log
    // ---------------------------------------------------------------
    private void log(String msg) {
        System.out.printf("[NÓ %-8s %s] %s%n",
                nodeId, new Timestamp(System.currentTimeMillis()), msg);
    }

    // ---------------------------------------------------------------
    // Main para testar os três nós localmente em threads separadas
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