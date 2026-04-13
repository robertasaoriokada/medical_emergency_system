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

    private static final int  CLIENT_PORT        = 9000;  // porta onde clientes (TCPClient) se conectam
    private static final int  THREAD_POOL        = 10;    // máximo de clientes atendidos ao mesmo tempo
    private static final int  DISPATCH_POOL      = 5;     // threads que ficam despachando ocorrências aos nós
    private static final long DISPATCH_MS        = 100;   // pausa entre tentativas quando nenhum nó está disponível

    /**
     * Tempo (ms) sem despacho após o qual a prioridade de uma ocorrência
     * é promovida automaticamente (aging), evitando starvation das
     * ocorrências menos urgentes quando o sistema está sobrecarregado.
     *
     * Ex: uma ocorrência prioridade 4 (VERDE) que aguarda mais de
     * AGING_THRESHOLD_MS sem ser despachada tem sua prioridade efetiva
     * elevada para 3, depois 2, até ser atendida.
     */
    private static final long AGING_THRESHOLD_MS = 30_000; // a cada 30s na fila, a prioridade sobe 1 nível

    private static DatabaseManager db;

    private static final Map<String, Node>          registeredNodes = new ConcurrentHashMap<>(); // nodeId → objeto Node
    private static final Map<String, AtomicInteger> nodeLoad        = new ConcurrentHashMap<>(); // nodeId → ocorrências ativas

    /**
     * Fila de prioridade central.
     * Ordenada pelo compareTo de Occurrence: prioridade 1 sai primeiro.
     * O aging é aplicado no momento da seleção de candidatos, sem
     * modificar o campo imutável `priority` — usamos effectivePriority()
     * que leva em conta o tempo de espera.
     *
     * PriorityBlockingQueue: auto-ordena pelo compareTo de Occurrence e
     * bloqueia a thread que tenta retirar algo quando está vazia,
     * sem desperdiçar CPU.
     */
    private static final PriorityBlockingQueue<Occurrence> priorityQueue =
            new PriorityBlockingQueue<>();

    /**
     * Pool de threads para atender conexões de clientes (TCPClient).
     * FixedThreadPool cria exatamente N threads e as mantém vivas;
     * tarefas excedentes ficam na fila interna do pool.
     * Threads daemon morrem junto com o processo principal.
     */
    private static final ExecutorService clientPool =
            Executors.newFixedThreadPool(THREAD_POOL, r -> {
                Thread t = new Thread(r, "client-worker-" + System.nanoTime());
                t.setDaemon(true);
                return t;
            });

    /** Pool de threads que consomem a fila e despacham ocorrências aos nós. */
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

        /*
         * Registro dos nós com suas capacidades de prioridade:
         *
         *   SAMU_1  → maxPriority=1: atende TUDO, incluindo prioridade 1 (vermelho)
         *   SAMU_2  → maxPriority=2: atende prioridades 2–5 (laranja a azul)
         *   UPA_SUL → maxPriority=3: atende prioridades 3–5 (amarelo a azul)
         *
         * Assim, paradas cardíacas (prioridade 1) só vão para SAMU_1.
         * Casos urgentes (prioridade 2) vão para SAMU_1 ou SAMU_2.
         * Casos não urgentes (prioridade 3-5) podem ir para qualquer nó.
         */
        registerNode("SAMU_1",  "Ambulância SAMU 1 – UTI Móvel", "AMBULANCE", "localhost", 9100, 1);
        registerNode("SAMU_2",  "Ambulância SAMU 2",              "AMBULANCE", "localhost", 9102, 2);
        registerNode("UPA_SUL", "UPA Zona Sul",                   "UPA",       "localhost", 9101, 3);

        for (int i = 0; i < DISPATCH_POOL; i++) {
            dispatchPool.submit(TCPService::dispatchLoop);
        }

        new HeartbeatReceiver(registeredNodes, db).start(); // sobe antes — thread daemon, não bloqueia
        startClientServer();                         // bloqueia aqui, por isso fica por último
    }

    // ---------------------------------------------------------------
    // Servidor de clientes (porta 9000)
    // ---------------------------------------------------------------
    private static void startClientServer() {
        try (ServerSocket server = new ServerSocket(CLIENT_PORT)) {
            server.setReuseAddress(true);
            log("Servidor central iniciado na porta " + CLIENT_PORT);
            log("Distribuição por prioridade ativa | Aging: " + AGING_THRESHOLD_MS + "ms");

            while (true) {
                Socket clientSocket = server.accept(); // bloqueia até um cliente conectar
                clientPool.submit(() -> handleClient(clientSocket)); // delega a uma thread do pool
            }

        } catch (IOException e) {
            log("ERRO no servidor: " + e.getMessage());
        }
    }

    /**
     * Trata uma conexão de cliente recebida.
     * Lê o objeto Occurrence serializado, persiste no banco,
     * enfileira na fila de prioridade e devolve ACK ao cliente.
     *
     * Ordem OOS → OIS é obrigatória para evitar deadlock no handshake
     * do ObjectStream: cada lado precisa enviar seu cabeçalho antes
     * de tentar ler o do outro.
     */
private static void handleClient(Socket clientSocket) {
    String clientAddr = clientSocket.getInetAddress().getHostAddress();
    log("Cliente conectado: " + clientAddr);

    try (
        ObjectOutputStream out = new ObjectOutputStream(clientSocket.getOutputStream());
        ObjectInputStream  in  = new ObjectInputStream(clientSocket.getInputStream())
    ) {
        out.flush();

        Object obj = in.readObject();

        if (!(obj instanceof Occurrence occurrence)) {
            out.writeObject("ERR:objeto inválido");
            return;
        }

        log("Ocorrência recebida: " + occurrence);

        System.out.println("[DEBUG] Antes de salvar no banco");

        db.saveOccurrence(occurrence);

        System.out.println("[DEBUG] Depois de salvar no banco");

        priorityQueue.offer(occurrence);

        out.writeObject("ACK:" + occurrence.getId());
        out.flush();
        
        log("ACK enviado → " + occurrence.getId().substring(0, 8));

    } catch (Exception e) {
        System.err.println("ERRO REAL no servidor:");
        e.printStackTrace();

        try {
            ObjectOutputStream out = new ObjectOutputStream(clientSocket.getOutputStream());
            out.writeObject("ERR:" + e.getMessage());
            out.flush();
        } catch (Exception ignored) {}

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
                Occurrence occurrence = priorityQueue.take(); // bloqueia se vazia
                dispatchToNode(occurrence);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }

    private static void dispatchToNode(Occurrence occurrence) {

        // Calcula prioridade efetiva considerando aging
        int effectivePriority = effectivePriority(occurrence);

        if (effectivePriority < occurrence.getPriority()) {
            log(String.format("AGING aplicado: ocorrência %s promovida P%d → P%d (aguardando %ds)",
                    occurrence.getId().substring(0, 8),
                    occurrence.getPriority(),
                    effectivePriority,
                    waitSeconds(occurrence)));
        }

        List<Node> candidates = selectCandidates(occurrence, effectivePriority);

        if (candidates.isEmpty()) {
            log("SEM NÓ DISPONÍVEL para " + occurrence.getId().substring(0, 8)
                    + " (P" + effectivePriority + ") — recolocando na fila");
            occurrence.setStatus(Status.PENDING);
            priorityQueue.offer(occurrence);
            try { Thread.sleep(DISPATCH_MS); } catch (InterruptedException ignored) {}
            return;
        }

        Node target = leastLoaded(candidates);

        // Incrementa ANTES de tentar — se falhar, o valor já está correto para o próximo ciclo
        occurrence.incrementDispatchAttempts();

        log(String.format("Despachando %s (P%d efetivo) → %s [carga=%d | maxP=%d | tentativa=%d]",
                occurrence.getId().substring(0, 8),
                effectivePriority,
                target.id,
                nodeLoad.get(target.id).get(),
                target.getMaxPriority(),
                occurrence.getDispatchAttempts()));

        Timestamp dispatchedAt = new Timestamp(System.currentTimeMillis());
        occurrence.setAssignedNode(target.id);
        occurrence.setStatus(Status.DISPATCHED);
        db.updateOccurrenceStatus(occurrence.getId(), Status.DISPATCHED.name());

        boolean acked = sendToNode(target, occurrence);
        Timestamp ackAt = new Timestamp(System.currentTimeMillis());

        if (acked) {
            occurrence.setStatus(Status.ACKNOWLEDGED);
            db.updateOccurrenceStatus(occurrence.getId(), Status.ACKNOWLEDGED.name());

            // Marca COMPLETED com timestamp exato do ACK
            db.updateOccurrenceCompleted(occurrence.getId(), ackAt);

            long responseMs = ackAt.getTime() - dispatchedAt.getTime();
            int retries = occurrence.getDispatchAttempts() - 1;
            db.saveMetric(occurrence.getId(), target.id, dispatchedAt, ackAt, responseMs, retries);

            log(String.format("ACK confirmado de %s em %dms | retries=%d",
                    target.id, responseMs, retries));

        } else {
            log("FALHA no nó " + target.id + " — marcando OFFLINE, recolocando ocorrência");
            markNodeUnavailable(target.id);
            // nodeLoad já foi decrementado dentro de sendToNode ao lançar exceção
            occurrence.setStatus(Status.PENDING);
            occurrence.setAssignedNode(null);
            priorityQueue.offer(occurrence);
        }
    }

    // ---------------------------------------------------------------
    // Aging — eleva prioridade de ocorrências com longa espera
    // ---------------------------------------------------------------

    /**
     * Calcula a prioridade efetiva de uma ocorrência levando em conta
     * o tempo que ela está na fila (aging).
     *
     * A cada AGING_THRESHOLD_MS de espera, a prioridade sobe 1 nível
     * (valor diminui 1), até o máximo de prioridade 1 (crítico).
     *
     * Exemplos com AGING_THRESHOLD_MS = 30s:
     *   - P5, esperando 0s  → efetiva P5
     *   - P5, esperando 35s → efetiva P4
     *   - P5, esperando 65s → efetiva P3
     *   - P3, esperando 35s → efetiva P2
     */
    private static int effectivePriority(Occurrence occurrence) {
        long waitMs = System.currentTimeMillis() - occurrence.getCreatedAt().getTime();
        int promotions = (int) (waitMs / AGING_THRESHOLD_MS);
        return Math.max(1, occurrence.getPriority() - promotions);
    }

    private static long waitSeconds(Occurrence occurrence) {
        return (System.currentTimeMillis() - occurrence.getCreatedAt().getTime()) / 1000;
    }

    // ---------------------------------------------------------------
    // Seleção de candidatos por prioridade
    // ---------------------------------------------------------------

    /**
     * Seleciona os nós aptos a atender uma ocorrência, considerando:
     *
     * 1. Disponibilidade (online)
     * 2. Capacidade de prioridade: node.canHandle(effectivePriority)
     *    — um nó com maxPriority=3 NÃO recebe ocorrências P1 ou P2
     * 3. Preferência por tipo para emergências graves:
     *    CARDIAC_ARREST, STROKE e TRAUMA preferem AMBULANCE
     *
     * Resultado: lista com nós preferidos no início, demais no fim.
     */
    private static List<Node> selectCandidates(Occurrence occurrence, int effectivePriority) {
        List<Node> preferred = new ArrayList<>();
        List<Node> fallback  = new ArrayList<>();

        boolean preferAmbulance =
                occurrence.getType() == Occurrence.Type.CARDIAC_ARREST
                || occurrence.getType() == Occurrence.Type.STROKE
                || occurrence.getType() == Occurrence.Type.TRAUMA;

        for (Node node : registeredNodes.values()) {
            if (!node.available) continue;
            if (!node.canHandle(effectivePriority)) continue;

            if (preferAmbulance && node.type.equals("AMBULANCE")) {
                preferred.add(node);
            } else {
                fallback.add(node);
            }
        }

        List<Node> result = new ArrayList<>(preferred);
        result.addAll(fallback);
        return result;
    }

    // ---------------------------------------------------------------
    // Balanceamento: menor carga entre os candidatos
    // ---------------------------------------------------------------
    private static Node leastLoaded(List<Node> candidates) {
        return candidates.stream()
                .min(Comparator.comparingInt(n -> nodeLoad.get(n.id).get()))
                .orElse(candidates.get(0));
    }

    // ---------------------------------------------------------------
    // Comunicação com nós
    // ---------------------------------------------------------------

    /**
     * Abre uma conexão TCP com o nó, envia a Occurrence serializada
     * e aguarda o ACK de confirmação.
     *
     * ObjectOutputStream ANTES de ObjectInputStream: garante que o
     * cabeçalho de 4 bytes do OOS seja enviado antes de tentar ler
     * o cabeçalho do outro lado — evita deadlock mútuo no handshake.
     */
private static boolean sendToNode(Node node, Occurrence occurrence) {
    try (Socket socket = new Socket()) {
        socket.connect(new InetSocketAddress(node.host, node.port), 3000);
        socket.setSoTimeout(5000);

        // Incrementa carga ANTES de enviar — reserva o slot no nó
        nodeLoad.get(node.id).incrementAndGet();

        ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
        out.flush();
        ObjectInputStream in = new ObjectInputStream(socket.getInputStream());

        out.writeObject(occurrence);
        out.flush();

        Object response = in.readObject();
        return response instanceof String s && s.startsWith("ACK:");

    } catch (Exception e) {
        // Reverte o incremento se falhou antes do envio
        nodeLoad.get(node.id).decrementAndGet();
        log("ERRO ao enviar para nó " + node.id + ": " + e.getMessage());
        return false;
    }
}
    // ---------------------------------------------------------------
    // Gerenciamento de nós
    // ---------------------------------------------------------------

    /** Registra um nó sem maxPriority — atende todas as prioridades (maxPriority = 5). */
    public static void registerNode(String id, String name,
                                     String type, String host, int port) {
        registerNode(id, name, type, host, port, 5);
    }

    /** Registra um nó com capacidade de prioridade definida. */
    public static void registerNode(String id, String name,
                                     String type, String host, int port,
                                     int maxPriority) {
        Node node = new Node(id, name, type, host, port, maxPriority);
        registeredNodes.put(id, node);
        nodeLoad.put(id, new AtomicInteger(0));
        db.registerNode(id, name, type);
        log("Nó registrado: " + node);
    }

    public static void markNodeUnavailable(String nodeId) {
        Node node = registeredNodes.get(nodeId);
        if (node != null) {
            node.available = false;
            db.markNodeOffline(nodeId);
            log("Nó marcado OFFLINE: " + nodeId);
        }
    }

    public static void markNodeAvailable(String nodeId) {
        Node node = registeredNodes.get(nodeId);
        if (node != null) {
            node.available = true;
            nodeLoad.put(nodeId, new AtomicInteger(0));
            db.updateHeartbeat(nodeId);
            log("Nó reativado: " + nodeId);
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