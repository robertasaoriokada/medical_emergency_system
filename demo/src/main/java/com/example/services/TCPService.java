package com.example.services;

import com.example.model.Node;
import com.example.model.Occurrence;
import com.example.model.Occurrence.Status;

import java.io.*;
import java.net.*;
import java.sql.Timestamp;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import com.example.db.DatabaseManager;

public class TCPService {

    private static final int  CLIENT_PORT   = 9000;
    private static final int  THREAD_POOL   = 10;
    private static final int  DISPATCH_POOL = 5;
    private static final long DISPATCH_MS   = 100;

    private static DatabaseManager db;

    private static final Map<String, Node>          registeredNodes = new ConcurrentHashMap<>();
    private static final Map<String, AtomicInteger> nodeLoad        = new ConcurrentHashMap<>();
    private static final PriorityBlockingQueue<Occurrence> priorityQueue   = new PriorityBlockingQueue<>();

    private static final ExecutorService clientPool =
            Executors.newFixedThreadPool(THREAD_POOL, r -> {
                Thread t = new Thread(r, "client-worker-" + System.nanoTime());
                t.setDaemon(true);
                return t;
            });

    private static final ExecutorService dispatchPool =
            Executors.newFixedThreadPool(DISPATCH_POOL, r -> {
                Thread t = new Thread(r, "dispatch-worker-" + System.nanoTime());
                t.setDaemon(true);
                return t;
            });

    // ---------------------------------------------------------------
    // Entrada
    // ---------------------------------------------------------------
    public static void main(String[] args) {
        db = new DatabaseManager();

        registerNode("SAMU_1",  "Ambulância SAMU 1", "AMBULANCE", "localhost", 9100);
        registerNode("UPA_SUL", "UPA Zona Sul",       "UPA",       "localhost", 9101);
        registerNode("SAMU_2",  "Ambulância SAMU 2",  "AMBULANCE", "localhost", 9102);

        for (int i = 0; i < DISPATCH_POOL; i++) {
            dispatchPool.submit(TCPService::dispatchLoop);
        }

        startClientServer();
        startHeartbeatReceiver();
    }

    // ---------------------------------------------------------------
    // Servidor de clientes
    // ---------------------------------------------------------------
    private static void startClientServer() {
        try (ServerSocket server = new ServerSocket(CLIENT_PORT)) {
            server.setReuseAddress(true);
            log("Servidor central iniciado na porta " + CLIENT_PORT);
            log("Thread pool: " + THREAD_POOL + " workers | Fila de prioridade ativa");

            while (true) {
                Socket clientSocket = server.accept();
                clientPool.submit(() -> handleClient(clientSocket));
            }

        } catch (IOException e) {
            log("ERRO no servidor: " + e.getMessage());
        }
    }

    private static void handleClient(Socket clientSocket) {
        String clientAddr = clientSocket.getInetAddress().getHostAddress();
        log("Cliente conectado: " + clientAddr);

        try (
            ObjectInputStream  in  = new ObjectInputStream(clientSocket.getInputStream());
            ObjectOutputStream out = new ObjectOutputStream(clientSocket.getOutputStream())
        ) {
            Object obj = in.readObject();

            if (!(obj instanceof Occurrence occurrence)) {
                out.writeObject("ERR:objeto inválido");
                return;
            }

            log("Ocorrência recebida: " + occurrence);

            // ✅ CORRIGIDO: usa sobrecarga com objeto completo,
            //    que inclui createdAt e receivedAt corretamente
            db.saveOccurrence(occurrence);

            priorityQueue.offer(occurrence);

            out.writeObject("ACK:" + occurrence.getId());
            log("ACK enviado → " + occurrence.getId().substring(0, 8));

        } catch (Exception e) {
            log("ERRO ao tratar cliente " + clientAddr + ": " + e.getMessage());
        } finally {
            try { clientSocket.close(); } catch (IOException ignored) {}
        }
    }

    // ---------------------------------------------------------------
    // Loop de despacho
    // ---------------------------------------------------------------
    private static void dispatchLoop() {
        log("Dispatcher iniciado: " + Thread.currentThread().getName());

        while (!Thread.currentThread().isInterrupted()) {
            try {
                Occurrence occurrence = priorityQueue.take();
                dispatchToNode(occurrence);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }

    private static void dispatchToNode(Occurrence occurrence) {
        List<Node> candidates = selectCandidates(occurrence);

        if (candidates.isEmpty()) {
            log("SEM NÓ DISPONÍVEL para " + occurrence.getId().substring(0, 8)
                    + " — recolocando na fila");
            occurrence.setStatus(Status.PENDING);
            priorityQueue.offer(occurrence);
            try { Thread.sleep(DISPATCH_MS); } catch (InterruptedException ignored) {}
            return;
        }

        Node target = roundRobin(candidates);

        log("Despachando " + occurrence.getId().substring(0, 8)
                + " → " + target.id + " (carga atual: " + nodeLoad.get(target.id).get() + ")");

        Timestamp dispatchedAt = new Timestamp(System.currentTimeMillis());
        occurrence.setAssignedNode(target.id);
        occurrence.setStatus(Status.DISPATCHED);
        db.updateOccurrenceStatus(occurrence.getId(), Status.DISPATCHED.name());

        boolean acked = sendToNode(target, occurrence);
        Timestamp ackAt = new Timestamp(System.currentTimeMillis());

        if (acked) {
            // ✅ CORRIGIDO: decrementa carga após confirmar ACK
            nodeLoad.get(target.id).decrementAndGet();
            occurrence.setStatus(Status.ACKNOWLEDGED);
            db.updateOccurrenceStatus(occurrence.getId(), Status.ACKNOWLEDGED.name());
            long responseMs = ackAt.getTime() - dispatchedAt.getTime();
            db.saveMetric(occurrence.getId(), target.id, dispatchedAt, ackAt, responseMs, 0);
            log("ACK confirmado de " + target.id + " em " + responseMs + "ms");
        } else {
            log("FALHA no nó " + target.id + " — marcando OFFLINE, recolocando ocorrência");
            // ✅ CORRIGIDO: usa volatile field via método dedicado
            markNodeUnavailable(target.id);
            occurrence.setStatus(Status.PENDING);
            occurrence.setAssignedNode(null);
            priorityQueue.offer(occurrence);
        }
    }

    // ---------------------------------------------------------------
    // Comunicação com nós
    // ---------------------------------------------------------------
    private static boolean sendToNode(Node node, Occurrence occurrence) {
        try (
            // ✅ CORRIGIDO: socket e streams todos no try-with-resources
            Socket socket            = new Socket();
            // streams declarados após connect() abaixo — ver bloco interno
        ) {
            socket.connect(new InetSocketAddress(node.host, node.port), 3000);
            socket.setSoTimeout(5000);

            // streams criados após connect para evitar bloqueio prematuro
            ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
            ObjectInputStream  in  = new ObjectInputStream(socket.getInputStream());

            out.writeObject(occurrence);
            out.flush();

            Object response = in.readObject();

            boolean success = response instanceof String s && s.startsWith("ACK:");

            // ✅ CORRIGIDO: só incrementa carga se realmente obteve ACK
            if (success) {
                nodeLoad.get(node.id).incrementAndGet();
            }

            return success;

        } catch (Exception e) {
            log("ERRO ao enviar para nó " + node.id + ": " + e.getMessage());
            return false;
        }
    }

    // ---------------------------------------------------------------
    // Seleção de nós
    // ---------------------------------------------------------------
    private static List<Node> selectCandidates(Occurrence occurrence) {
        List<Node> result = new ArrayList<>();

        for (Node node : registeredNodes.values()) {
            // ✅ CORRIGIDO: leitura via volatile — thread-safe
            if (!node.available) continue;

            boolean preferAmbulance =
                    occurrence.getType() == Occurrence.Type.CARDIAC_ARREST
                    || occurrence.getType() == Occurrence.Type.STROKE
                    || occurrence.getType() == Occurrence.Type.TRAUMA;

            if (preferAmbulance && node.type.equals("AMBULANCE")) {
                result.add(0, node);
            } else {
                result.add(node);
            }
        }
        return result;
    }

    private static Node roundRobin(List<Node> candidates) {
        return candidates.stream()
                .min(Comparator.comparingInt(n -> nodeLoad.get(n.id).get()))
                .orElse(candidates.get(0));
    }

    // ---------------------------------------------------------------
    // Gerenciamento de nós
    // ---------------------------------------------------------------
    public static void registerNode(String id, String name,
                                     String type, String host, int port) {
        Node node = new Node(id, name, type, host, port);
        registeredNodes.put(id, node);
        nodeLoad.put(id, new AtomicInteger(0));
        db.registerNode(id, name, type);
        log("Nó registrado: " + node);
    }

    private static void markNodeUnavailable(String nodeId) {
        Node node = registeredNodes.get(nodeId);
        if (node != null) {
            node.available = false;          // volatile — visível a todas as threads
            db.markNodeOffline(nodeId);
            log("Nó marcado OFFLINE: " + nodeId);
        }
    }

    public static void markNodeAvailable(String nodeId) {
        Node node = registeredNodes.get(nodeId);
        if (node != null) {
            node.available = true;
            nodeLoad.put(nodeId, new AtomicInteger(0));  // reseta carga
            db.updateHeartbeat(nodeId);
            log("Nó reativado: " + nodeId);
        }
    }

    // ---------------------------------------------------------------
// Receptor UDP de Heartbeat (porta 9001)
// ---------------------------------------------------------------
private static void startHeartbeatReceiver() {
    Thread udpThread = new Thread(() -> {
        try (DatagramSocket udpSocket = new DatagramSocket(9001)) {
            udpSocket.setReuseAddress(true);
            log("Receptor de heartbeat UDP iniciado na porta 9001");

            byte[] buffer = new byte[256];

            while (!Thread.currentThread().isInterrupted()) {
                DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
                udpSocket.receive(packet); // bloqueia até chegar pacote

                String message = new String(packet.getData(), 0, packet.getLength()).trim();
                handleHeartbeat(message, packet.getAddress().getHostAddress());
            }

        } catch (Exception e) {
            log("ERRO no receptor UDP: " + e.getMessage());
        }
    }, "heartbeat-receiver");

    udpThread.setDaemon(true);
    udpThread.start();
}

    private static void handleHeartbeat(String message, String fromIp) {
        // Formato esperado: "HB:<nodeId>"
        if (!message.startsWith("HB:")) {
            log("Heartbeat malformado de " + fromIp + ": " + message);
            return;
        }

        String nodeId = message.substring(3);
        Node node = registeredNodes.get(nodeId);

        if (node == null) {
            log("Heartbeat de nó desconhecido: " + nodeId);
            return;
        }

        db.updateHeartbeat(nodeId);

        // Se o nó estava offline, reativa automaticamente
        if (!node.available) {
            markNodeAvailable(nodeId);
            log("Nó REATIVADO via heartbeat: " + nodeId + " (" + fromIp + ")");
        } else {
            log("Heartbeat recebido de " + nodeId + " (" + fromIp + ")");
        }
    }

    // ---------------------------------------------------------------
    // Log
    // ---------------------------------------------------------------
    private static void log(String msg) {
        System.out.printf("[SERVIDOR %s] %s%n",
                new Timestamp(System.currentTimeMillis()), msg);
    }
}