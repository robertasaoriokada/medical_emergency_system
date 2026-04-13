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

    private static final int  CLIENT_PORT   = 9000; // porta onde clientes (TCPClient) se conectam
    private static final int  THREAD_POOL   = 10; // máximo de clientes atendidos ao mesmo tempo
    private static final int  DISPATCH_POOL = 5; // threads que ficam despachando ocorrências aos nós
    private static final long DISPATCH_MS   = 100; // pausa entre tentativas quando nenhum nó está disponível

    /**
     * Tempo (ms) sem despacho após o qual a prioridade de uma ocorrência
     * é promovida automaticamente (aging), evitando starvation das
     * ocorrências menos urgentes quando o sistema está sobrecarregado.
     *
     * Ex: uma ocorrência prioridade 4 (VERDE) que aguarda mais de
     * AGING_THRESHOLD_MS sem ser despachada tem sua prioridade efetiva
     * elevada para 3, depois 2, até ser atendida.
     */
    private static final long AGING_THRESHOLD_MS = 30_000; // 30 segundos, a cada 30s na fila, a prioridade sobe 1 nível

    private static DatabaseManager db;

    private static final Map<String, Node> registeredNodes = new ConcurrentHashMap<>(); // nodeId → objeto Node
    private static final Map<String, AtomicInteger> nodeLoad        = new ConcurrentHashMap<>(); // nodeId → quantidade de ocorrências ativas

    /**
     * Fila de prioridade central.
     * Ordenada pelo compareTo de Occurrence: prioridade 1 sai primeiro.
     * O aging é aplicado no momento da seleção de candidatos, sem
     * modificar o campo imutável `priority` — usamos `effectivePriority()`
     * que leva em conta o tempo de espera.
     */
    private static final PriorityBlockingQueue<Occurrence> priorityQueue =
            new PriorityBlockingQueue<>(); // fila central de ocorrências
    //Se auto ordena pelo compareTo de Occurence e bloqueia a thread que tenta retirar algo quando está vazia, sem desperdiçar a CPU.


    //Em vez de criar uma thread nova para cada conexão, o servidor mantém pools de threads reutilizáveis.
    //FixedThreadPool cria exatamente N threds e as mantém vivas, se chegarem mais tarefas elas ficam esperando
    private static final ExecutorService clientPool =
            Executors.newFixedThreadPool(THREAD_POOL, r -> {
                Thread t = new Thread(r, "client-worker-" + System.nanoTime());
                t.setDaemon(true); //serve para quando o processo principal encerrar, essas threads morrem automaticamente sem precisar de um shutdown explícito
                return t;
            });


    //Envia as ocorrência aos nós
    private static final ExecutorService dispatchPool =
            Executors.newFixedThreadPool(DISPATCH_POOL, r -> {
                Thread t = new Thread(r, "dispatch-worker-" + System.nanoTime());
                t.setDaemon(true);
                return t;
            });


    // ---------------------------------------------------------------
    // Servidor de clientes
    // ---------------------------------------------------------------
    private static void startClientServer() {
        try (ServerSocket server = new ServerSocket(CLIENT_PORT)) {  // abre a porta e fica escutando
            server.setReuseAddress(true);
            log("Servidor central iniciado na porta " + CLIENT_PORT);
            log("Distribuição por prioridade ativa | Aging: " + AGING_THRESHOLD_MS + "ms");

            while (true) {
                Socket clientSocket = server.accept(); // bloqueia até um cliente conectar → retorna Socket
                clientPool.submit(() -> handleClient(clientSocket)); // delega o atendimento a uma thread do pool
            }

        } catch (IOException e) {
            log("ERRO no servidor: " + e.getMessage());
        }
    }

    //Recebe um socket cliente
    private static void handleClient(Socket clientSocket) {
        String clientAddr = clientSocket.getInetAddress().getHostAddress(); //pega o endereço de host do cliente
        log("Cliente conectado: " + clientAddr);

        try (
            ObjectInputStream  in  = new ObjectInputStream(clientSocket.getInputStream()); //instancia um objeto recuperando-o do stream
            ObjectOutputStream out = new ObjectOutputStream(clientSocket.getOutputStream()) //escreve um objeto no stream
        ) {
            Object obj = in.readObject(); //espera por um retorno

            if (!(obj instanceof Occurrence occurrence)) { //verifica se o objeto recebido é um occurence, senão ele escreve no buffer um objeto inválido
                out.writeObject("ERR:objeto inválido");
                return;
            } 

            log("Ocorrência recebida: " + occurrence);
            db.saveOccurrence(occurrence); //salvar no banco a ocorrência
            priorityQueue.offer(occurrence); //faz a lógica de prioridade

            out.writeObject("ACK:" + occurrence.getId()); //serializa no stream e joga no buffer o Acknowledgement 
            //simplemente estamos confirmando o recebimento e que o Occurence foi salvo no banco.
            //no caso nao temos como ver, somente com wireshark e tudo mais
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

        log(String.format("Despachando %s (P%d efetivo) → %s [carga=%d | maxP=%d]",
                occurrence.getId().substring(0, 8),
                effectivePriority,
                target.id,
                nodeLoad.get(target.id).get(),
                target.getMaxPriority()));

        Timestamp dispatchedAt = new Timestamp(System.currentTimeMillis());
        occurrence.setAssignedNode(target.id);
        occurrence.setStatus(Status.DISPATCHED);
        db.updateOccurrenceStatus(occurrence.getId(), Status.DISPATCHED.name());

        boolean acked = sendToNode(target, occurrence);
        Timestamp ackAt = new Timestamp(System.currentTimeMillis());

        if (acked) {
            nodeLoad.get(target.id).decrementAndGet();
            occurrence.setStatus(Status.ACKNOWLEDGED);
            db.updateOccurrenceStatus(occurrence.getId(), Status.ACKNOWLEDGED.name());
            long responseMs = ackAt.getTime() - dispatchedAt.getTime();
            db.saveMetric(occurrence.getId(), target.id, dispatchedAt, ackAt, responseMs, 0);
            log("ACK confirmado de " + target.id + " em " + responseMs + "ms");
        } else {
            log("FALHA no nó " + target.id + " — marcando OFFLINE, recolocando ocorrência");
            markNodeUnavailable(target.id);
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
     * Resultado: lista ordenada com nós preferidos no início.
     */
    private static List<Node> selectCandidates(Occurrence occurrence, int effectivePriority) {
        List<Node> preferred = new ArrayList<>();
        List<Node> fallback  = new ArrayList<>();

        boolean preferAmbulance =
                occurrence.getType() == Occurrence.Type.CARDIAC_ARREST
                || occurrence.getType() == Occurrence.Type.STROKE
                || occurrence.getType() == Occurrence.Type.TRAUMA;

        for (Node node : registeredNodes.values()) {

            // Nó offline → descarta
            if (!node.available) continue;

            // Nó não tem capacidade para esta prioridade → descarta
            if (!node.canHandle(effectivePriority)) continue;

            if (preferAmbulance && node.type.equals("AMBULANCE")) {
                preferred.add(node);   // ambulâncias na frente para emergências graves
            } else {
                fallback.add(node);
            }
        }

        // Une: nós preferidos primeiro, depois os demais
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
    private static boolean sendToNode(Node node, Occurrence occurrence) {
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(node.host, node.port), 3000);
            socket.setSoTimeout(5000);

            ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
            ObjectInputStream  in  = new ObjectInputStream(socket.getInputStream());

            out.writeObject(occurrence);
            out.flush();

            Object response = in.readObject();
            boolean success = response instanceof String s && s.startsWith("ACK:");

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
    // Gerenciamento de nós
    // ---------------------------------------------------------------

    /** Registra um nó sem maxPriority — atende todas as prioridades. */
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



    public static void main(String[] args) {
        db = new DatabaseManager();

        registerNode("SAMU_1",  "Ambulância SAMU 1 – UTI Móvel", "AMBULANCE", "localhost", 9100, 1);
        registerNode("SAMU_2",  "Ambulância SAMU 2",              "AMBULANCE", "localhost", 9102, 2);
        registerNode("UPA_SUL", "UPA Zona Sul",                   "UPA",       "localhost", 9101, 3);

        for (int i = 0; i < DISPATCH_POOL; i++) {
            dispatchPool.submit(TCPService::dispatchLoop);
        }

        new Heartbeat(registeredNodes, db).start(); // sobe antes — thread daemon, não bloqueia
        startClientServer();                         // bloqueia aqui, por isso fica por último
    }
}
