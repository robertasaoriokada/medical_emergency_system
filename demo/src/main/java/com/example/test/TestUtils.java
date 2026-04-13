package com.example.test;

import com.example.model.Occurrence;
import com.example.model.Occurrence.Color;
import com.example.model.Occurrence.Type;

import java.util.List;
import java.util.Random;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Utilitários compartilhados entre os cenários de teste.
 *
 * - Geração aleatória de Occurrence
 * - Coleta de métricas em memória (latência, sucessos, falhas)
 * - Impressão de relatório final
 */
public class TestUtils {

    private static final Random RNG = new Random();

    // Pesos de probabilidade para cada prioridade (índice 0 = P1, índice 4 = P5)
    private static final int[] PRIORITY_WEIGHTS = {10, 20, 35, 25, 10};

    private static final Type[]  TYPES  = Type.values();
    private static final Color[] COLORS = {
        Color.RED, Color.ORANGE, Color.YELLOW, Color.GREEN, Color.BLUE
    };

    // ---------------------------------------------------------------
    // Geração de ocorrências
    // ---------------------------------------------------------------

    /** Gera uma Occurrence com prioridade distribuída por peso (P1 = rara, P3 = comum). */
    public static Occurrence randomOccurrence(String origin) {
        int priority = weightedPriority();
        Type  type   = TYPES[RNG.nextInt(TYPES.length)];
        Color color  = COLORS[priority - 1];
        String desc  = "Ocorrência de teste | origem=" + origin
                     + " | tipo=" + type + " | P" + priority;
        return new Occurrence(origin, type, priority, color, desc);
    }

    /** Gera uma Occurrence com prioridade fixa. */
    public static Occurrence fixedOccurrence(String origin, int priority, Type type) {
        Color color = COLORS[priority - 1];
        return new Occurrence(origin, type, priority, color,
                "Teste fixo | P" + priority + " | " + type);
    }

    private static int weightedPriority() {
        int total = 0;
        for (int w : PRIORITY_WEIGHTS) total += w;
        int roll = RNG.nextInt(total);
        int cumulative = 0;
        for (int i = 0; i < PRIORITY_WEIGHTS.length; i++) {
            cumulative += PRIORITY_WEIGHTS[i];
            if (roll < cumulative) return i + 1;
        }
        return 3;
    }

    // ---------------------------------------------------------------
    // Coleta de métricas
    // ---------------------------------------------------------------

    /**
     * Acumulador de métricas thread-safe.
     * Cada thread de teste registra seus resultados aqui.
     */
    public static class Metrics {
        private final AtomicInteger success  = new AtomicInteger(0);
        private final AtomicInteger failure  = new AtomicInteger(0);
        private final List<Long>    latencies = new CopyOnWriteArrayList<>();
        private final long startMs = System.currentTimeMillis();

        public void recordSuccess(long latencyMs) {
            success.incrementAndGet();
            latencies.add(latencyMs);
        }

        public void recordFailure() {
            failure.incrementAndGet();
        }

        public void printReport(String scenarioName) {
            long elapsedMs = System.currentTimeMillis() - startMs;
            int  total     = success.get() + failure.get();
            double throughput = total / (elapsedMs / 1000.0);

            System.out.println("\n" + "=".repeat(60));
            System.out.println("RELATÓRIO — " + scenarioName);
            System.out.println("=".repeat(60));
            System.out.printf("  Duração total       : %d ms%n", elapsedMs);
            System.out.printf("  Ocorrências enviadas: %d%n", total);
            System.out.printf("  Sucesso (ACK)       : %d%n", success.get());
            System.out.printf("  Falha               : %d%n", failure.get());
            System.out.printf("  Taxa de sucesso     : %.1f%%%n",
                    total == 0 ? 0 : 100.0 * success.get() / total);
            System.out.printf("  Vazão               : %.2f oc/s%n", throughput);

            if (!latencies.isEmpty()) {
                long sum = latencies.stream().mapToLong(Long::longValue).sum();
                long min = latencies.stream().mapToLong(Long::longValue).min().orElse(0);
                long max = latencies.stream().mapToLong(Long::longValue).max().orElse(0);
                long avg = sum / latencies.size();

                List<Long> sorted = latencies.stream().sorted().toList();
                long p95 = sorted.get((int) (sorted.size() * 0.95));
                long p99 = sorted.get((int) (sorted.size() * 0.99));

                System.out.printf("  Latência mín        : %d ms%n", min);
                System.out.printf("  Latência média      : %d ms%n", avg);
                System.out.printf("  Latência máx        : %d ms%n", max);
                System.out.printf("  Latência P95        : %d ms%n", p95);
                System.out.printf("  Latência P99        : %d ms%n", p99);
            }

            System.out.println("=".repeat(60) + "\n");
        }
    }
}