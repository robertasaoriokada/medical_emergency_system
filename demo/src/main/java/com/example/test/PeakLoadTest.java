package com.example.test;

import com.example.model.Occurrence;
import com.example.services.TCPClient;
import com.example.test.TestUtils.Metrics;

import java.util.concurrent.*;

/**
 * Cenário 1 — Pico de ocorrências simultâneas.
 *
 * Objetivo: verificar comportamento do sistema sob carga alta,
 * medindo vazão, latência e taxa de sucesso.
 *
 * O que valida nos requisitos:
 *   - "Comportamento em pico de eventos" (Req 7)
 *   - "Métricas de desempenho: tempo de resposta, vazão" (Req geral)
 *   - "Escalabilidade" (Req geral)
 *
 * Pré-condição: TCPService e os três AttendanceNodes devem estar rodando.
 *
 * Como executar:
 *   1. Inicie TCPService.main()
 *   2. Inicie AttendanceNode.main()
 *   3. Execute PeakLoadTest.main()
 */
public class PeakLoadTest {

    // Número de clientes simultâneos simulados
    private static final int CONCURRENT_CLIENTS = 10;

    // Ocorrências enviadas por cliente
    private static final int OCCURRENCES_PER_CLIENT = 5;

    // Pausa entre envios dentro de um mesmo cliente (ms)
    private static final long SEND_INTERVAL_MS = 200;

    public static void main(String[] args) throws InterruptedException {
        System.out.println("=== CENÁRIO 1: Pico de ocorrências ===");
        System.out.printf("Clientes simultâneos: %d | Oc. por cliente: %d | Total: %d%n%n",
                CONCURRENT_CLIENTS, OCCURRENCES_PER_CLIENT,
                CONCURRENT_CLIENTS * OCCURRENCES_PER_CLIENT);

        Metrics metrics = new Metrics();
        ExecutorService pool = Executors.newFixedThreadPool(CONCURRENT_CLIENTS);
        CountDownLatch latch = new CountDownLatch(CONCURRENT_CLIENTS);

        // Dispara todos os clientes ao mesmo tempo
        for (int i = 0; i < CONCURRENT_CLIENTS; i++) {
            final String origin = "CLIENTE_" + (i + 1);
            pool.submit(() -> {
                try {
                    runClient(origin, metrics);
                } finally {
                    latch.countDown();
                }
            });
        }

        // Aguarda todos terminarem (timeout de 2 minutos)
        boolean finished = latch.await(2, TimeUnit.MINUTES);
        pool.shutdown();

        if (!finished) {
            System.out.println("[AVISO] Timeout — nem todos os clientes concluíram.");
        }

        metrics.printReport("Cenário 1 — Pico de carga (" + CONCURRENT_CLIENTS + " clientes)");
    }

    private static void runClient(String origin, Metrics metrics) {
        TCPClient client = new TCPClient(origin);

        for (int i = 0; i < OCCURRENCES_PER_CLIENT; i++) {
            Occurrence oc = TestUtils.randomOccurrence(origin);
            long start = System.currentTimeMillis();

            try {
                client.send(oc);
                long latency = System.currentTimeMillis() - start;
                metrics.recordSuccess(latency);
                System.out.printf("[%s] ACK recebido em %dms | P%d | %s%n",
                        origin, latency, oc.getPriority(), oc.getType());
            } catch (Exception e) {
                metrics.recordFailure();
                System.out.printf("[%s] FALHA: %s%n", origin, e.getMessage());
            }

            // Pequena pausa entre envios para não sobrecarregar o buffer do servidor
            try { Thread.sleep(SEND_INTERVAL_MS); } catch (InterruptedException ignored) {}
        }
    }
}