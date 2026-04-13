package com.example.test;

import com.example.db.DatabaseManager;
import com.example.model.Occurrence;
import com.example.model.Occurrence.Type;
import com.example.services.TCPClient;
import com.example.test.TestUtils.Metrics;

/**
 * Cenário 3 — Aging e priorização (anti-starvation).
 *
 * Objetivo: verificar que ocorrências de baixa prioridade (P4, P5) não
 * ficam indefinidamente presas quando o sistema está saturado com P1/P2.
 *
 * Roteiro:
 *   1. Envia um lote de ocorrências críticas (P1) para saturar os nós.
 *   2. Em paralelo, envia ocorrências de baixa prioridade (P5).
 *   3. Aguarda o AGING_THRESHOLD_MS (30s por padrão — reduzir para teste).
 *   4. Verifica nos logs do servidor que as P5 foram promovidas (P4→P3→...).
 *
 * IMPORTANTE: Para este teste ser conclusivo em tempo razoável,
 * reduza AGING_THRESHOLD_MS no TCPService para 5_000 (5 segundos) antes
 * de executar — e restaure para 30_000 após o teste.
 *
 * O que valida nos requisitos:
 *   - "Classificação e priorização" (Req 2)
 *   - "Como evita perda de eventos críticos" (Req 6)
 *   - Mecanismo anti-starvation com aging documentado
 *
 * Pré-condição: TCPService e AttendanceNodes rodando.
 *   Reduzir AGING_THRESHOLD_MS = 5_000 no TCPService para teste rápido.
 */
public class AgingPriorityTest {
    private static DatabaseManager db = new DatabaseManager();

    // Quantas ocorrências críticas para "saturar" os nós primeiro
    private static final int CRITICAL_BURST = 10;

    // Quantas ocorrências de baixa prioridade (P5) enviadas em paralelo
    private static final int LOW_PRIORITY_COUNT = 6;

    // Tempo de observação após envio (deve ser > AGING_THRESHOLD_MS do servidor)
    private static final long OBSERVATION_WINDOW_MS = 40_000; // 40 segundos

    public static void main(String[] args) throws Exception {
        System.out.println("=== CENÁRIO 3: Aging e priorização ===");
        System.out.println("ATENÇÃO: Reduza AGING_THRESHOLD_MS para 5000ms no TCPService para teste rápido.\n");

        Metrics metrics = new Metrics();
        TCPClient client = new TCPClient("TESTE_AGING");

        // --- Fase 1: saturar com P1 ---
        System.out.println(">>> FASE 1: Enviando " + CRITICAL_BURST + " ocorrências P1 (saturar nós)");
        for (int i = 0; i < CRITICAL_BURST; i++) {
            Occurrence oc = TestUtils.fixedOccurrence("BURST", 1, Type.CARDIAC_ARREST);
            sendAndRecord(client, oc, metrics, "Fase 1");
            Thread.sleep(100);
        }

        // --- Fase 2: injeta P5 logo depois ---
        System.out.println("\n>>> FASE 2: Enviando " + LOW_PRIORITY_COUNT + " ocorrências P5 (baixa prioridade)");
        System.out.println("    Observe nos logs do servidor quando elas forem promovidas...\n");
        for (int i = 0; i < LOW_PRIORITY_COUNT; i++) {
            Occurrence oc = TestUtils.fixedOccurrence("AGING_TEST", 5, Type.GENERAL);
            sendAndRecord(client, oc, metrics, "Fase 2");
            Thread.sleep(200);
        }

        // --- Fase 3: aguarda e observa o aging ---
        System.out.println("\n>>> FASE 3: Aguardando " + (OBSERVATION_WINDOW_MS / 1000)
                + "s para observar aging nos logs do servidor...");
        System.out.println("    Procure por: 'AGING aplicado: ocorrência ... promovida P5 → P4'\n");

        long deadline = System.currentTimeMillis() + OBSERVATION_WINDOW_MS;
        int dots = 0;
        while (System.currentTimeMillis() < deadline) {
            Thread.sleep(2_000);
            System.out.print(".");
            if (++dots % 15 == 0) System.out.println();
        }
        System.out.println("\n");

        metrics.printReport("Cenário 3 — Aging e priorização");
        System.out.println("Verifique os logs do TCPService para confirmar as promoções de prioridade.");
    }

private static void sendAndRecord(TCPClient client, Occurrence oc, Metrics metrics, String phase) {
    long start = System.currentTimeMillis();

    try {
        client.send(oc);

        long latency = System.currentTimeMillis() - start;
        metrics.recordSuccess(latency);

        db.saveTestResult(
            "AGING_TEST",
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