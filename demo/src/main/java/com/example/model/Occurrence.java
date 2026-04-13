package com.example.model;

import java.io.Serializable;
import java.sql.Timestamp;
import java.util.UUID;

/**
 * Representa uma ocorrência de emergência médica.
 * Serializable para trafegar via ObjectOutputStream no socket TCP.
 *
 * Prioridade segue protocolo de triagem Manchester simplificado:
 *   1 = VERMELHO   (imediato   — risco de vida)
 *   2 = LARANJA    (muito urgente)
 *   3 = AMARELO    (urgente)
 *   4 = VERDE      (pouco urgente)
 *   5 = AZUL       (não urgente)
 */
public class Occurrence implements Serializable, Comparable<Occurrence> {

    private static final long serialVersionUID = 1L;

    public enum Type {
        CARDIAC_ARREST, STROKE, TRAUMA, RESPIRATORY, OBSTETRIC, PSYCHIATRIC, GENERAL
    }

    public enum Status {
        PENDING, DISPATCHED, ACKNOWLEDGED, COMPLETED, FAILED
    }

    public enum Color {
        RED, ORANGE, YELLOW, GREEN, BLUE
    }

    private final String    id;
    private final String    origin;
    private final Type      type;
    private final int       priority;
    private final String    description;
    private final Timestamp createdAt;
    private final Timestamp receivedAt;
    private       Color     color;
    private       Status    status;
    private       String    assignedNode;

    /**
     * Contador de tentativas de despacho pelo servidor central.
     * Incrementado em TCPService toda vez que o envio ao nó falha
     * e a ocorrência volta à fila. Persistido em metrics.retries.
     */
    private int dispatchAttempts = 0;

    public Occurrence(String origin, Type type, int priority,
                      Color color, String description) {
        this.id          = UUID.randomUUID().toString();
        this.origin      = origin;
        this.type        = type;
        this.priority    = priority;
        this.color       = color;
        this.description = description;
        this.createdAt   = new Timestamp(System.currentTimeMillis());
        this.receivedAt  = new Timestamp(System.currentTimeMillis());
        this.status      = Status.PENDING;
    }

    @Override
    public int compareTo(Occurrence other) {
        return Integer.compare(this.priority, other.priority);
    }

    // --- Getters ---
    public String    getId()               { return id; }
    public String    getOrigin()           { return origin; }
    public Type      getType()             { return type; }
    public int       getPriority()         { return priority; }
    public Color     getColor()            { return color; }
    public String    getDescription()      { return description; }
    public Timestamp getCreatedAt()        { return createdAt; }
    public Timestamp getReceivedAt()       { return receivedAt; }
    public Status    getStatus()           { return status; }
    public String    getAssignedNode()     { return assignedNode; }
    public int       getDispatchAttempts() { return dispatchAttempts; }

    // --- Setters de estado ---
    public void setStatus(Status status)              { this.status        = status; }
    public void setColor(Color color)                 { this.color         = color; }
    public void setAssignedNode(String assignedNode)  { this.assignedNode  = assignedNode; }

    /** Incrementa o contador de tentativas de despacho. Chamado pelo TCPService. */
    public void incrementDispatchAttempts()           { this.dispatchAttempts++; }

    @Override
    public String toString() {
        return String.format("[%s] origem=%-10s tipo=%-15s prioridade=%d (%s) status=%s",
                id.substring(0, 8), origin, type, priority, color, status);
    }
}