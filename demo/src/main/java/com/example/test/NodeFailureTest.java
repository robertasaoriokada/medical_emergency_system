package com.example.test;

import com.example.db.DatabaseManager;
import com.example.model.Occurrence;
import com.example.model.Occurrence.Type;
import com.example.services.TCPClient;
import com.example.services.TCPService;
import com.example.test.TestUtils.Metrics;

/**
 * Cenário 2 — Indisponibilidade parcial e recuperação.
 *
 * Objetivo: simular a queda de um nó durante o atendimento e verificar
 * que o sistema redireciona ocorrências e se recupera quando o nó volta.
 *
 * Roteiro:
 *   Fase 1 — Sistema normal: envia ocorrências P1 (críticas) → devem
 *             ir para SAMU_1 (único com maxP=1).
 *   Fase 2 — Simula queda de SAMU_1: marca o nó como OFFLINE via
 *             TCPService.markNodeUnavailable().
 *             Envia mais ocorrências P1 → não há nó disponível → ficam
 *             na fila e são recolocadas.
 *   Fase 3 — Recuperação: chama TCPService.markNodeAvailable() simulando
 *             o heartbeat que reativaria o nó.
 *             Envia novas ocorrências → devem ser despachadas normalmente.
 *
 * O que valida nos requisitos:
 *   - "Tolerância a falhas" (Req geral)
 *   - "Mecanismo de detecção de falha e reenvio" (Req 5)
 *   - "Como evita perda de eventos críticos" (Req 6)
 *   - "Comportamento em indisponibilidade parcial" (Req 7)
 *
 * Pré-condição: TCPService e AttendanceNodes rodando.
 *   ATENÇÃO: SAMU_1 precisa estar online no início e ser derrubado
 *   manualmente na Fase 2 (ou use o script StopNode.sh).
 */
public class NodeFailureTest {
    
    private static DatabaseManager db = new DatabaseManager();
    private static final String NODE_TO_KILL = "SAMU_1";
    private static final long   PHASE_PAUSE  = 5_000; // pausa entre fases (ms)
    
    public static void main(String[] args) throws Exception {
        System.out.println("=== CENÁRIO 2: Falha e recuperação de nó ===\n");
        Metrics metrics = new Metrics();
        TCPClient client = new TCPClient("TESTE_FALHA");

        // --- Fase 1: Sistema normal ---
        System.out.println(">>> FASE 1: Sistema normal — 5 ocorrências P1 (críticas)");
        for (int i = 0; i < 5; i++) {
            Occurrence oc = TestUtils.fixedOccurrence("FASE1", 1, Type.CARDIAC_ARREST);
            sendAndRecord(client, oc, metrics, "Fase 1");
            Thread.sleep(500);
        }

        // --- Fase 2: Derruba o nó ---
        System.out.println("\n>>> FASE 2: Marcando " + NODE_TO_KILL + " como OFFLINE");
        System.out.println("    (Em produção: derrubar o processo AttendanceNode manualmente)");
        TCPService.markNodeUnavailable(NODE_TO_KILL);

        Thread.sleep(PHASE_PAUSE);

        // Tenta enviar P1 sem o SAMU_1 disponível
        // As ocorrências P1 ficam presas na fila (nenhum outro nó tem maxP=1)
        System.out.println("\n    Enviando 3 ocorrências P1 sem SAMU_1...");
        for (int i = 0; i < 3; i++) {
            Occurrence oc = TestUtils.fixedOccurrence("FASE2", 1, Type.STROKE);
            sendAndRecord(client, oc, metrics, "Fase 2");
            Thread.sleep(500);
        }

        // Ocorrências de prioridade menor ainda devem ser atendidas por outros nós
        System.out.println("\n    Enviando 3 ocorrências P3 (devem ir para UPA_SUL ou SAMU_2)...");
        for (int i = 0; i < 3; i++) {
            Occurrence oc = TestUtils.fixedOccurrence("FASE2", 3, Type.GENERAL);
            sendAndRecord(client, oc, metrics, "Fase 2");
            Thread.sleep(500);
        }

        Thread.sleep(PHASE_PAUSE);

        // --- Fase 3: Recupera o nó ---
        System.out.println("\n>>> FASE 3: Reativando " + NODE_TO_KILL + " (simula heartbeat recebido)");
        TCPService.markNodeAvailable(NODE_TO_KILL);

        Thread.sleep(2_000); // aguarda o dispatcher perceber e redespachar

        System.out.println("\n    Enviando 5 ocorrências P1 após recuperação...");
        for (int i = 0; i < 5; i++) {
            Occurrence oc = TestUtils.fixedOccurrence("FASE3", 1, Type.CARDIAC_ARREST);
            sendAndRecord(client, oc, metrics, "Fase 3");
            Thread.sleep(500);
        }

        metrics.printReport("Cenário 2 — Falha e recuperação de " + NODE_TO_KILL);
    }

    private static void sendAndRecord(TCPClient client, Occurrence oc, Metrics metrics, String phase) {
        long start = System.currentTimeMillis();

        try {
            client.send(oc);

            long latency = System.currentTimeMillis() - start;
            metrics.recordSuccess(latency);

            db.saveTestResult(
                "NODE_FAILURE",
                phase,
                oc.getId(),
                oc.getPriority(),
                oc.getType().name(),
                true,
                latency
            );

            System.out.printf("ACK em %dms | P%d | %s%n",
                    latency, oc.getPriority(), oc.getType());

        } catch (Exception e) {
            metrics.recordFailure();

            db.saveTestResult(
                "AGING_TEST",
                phase,
                oc.getId(),
                oc.getPriority(),
                oc.getType().name(),
                false,
                -1
            );

            System.out.printf("FALHA: %s%n", e.getMessage());
        }
    }
}
