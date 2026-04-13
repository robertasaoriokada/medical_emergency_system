package com.example.db;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;

import com.example.model.Occurrence;
import com.example.model.Occurrence.Color;

/**
 * Gerenciador de persistência — wraps a conexão JDBC e expõe
 * operações de domínio para o sistema de emergência.
 *
 * Schema esperado (MySQL):
 *
 *   CREATE TABLE occurrences (
 *     id           VARCHAR(36)  PRIMARY KEY,
 *     origin       VARCHAR(20)  NOT NULL,
 *     type         VARCHAR(30)  NOT NULL,
 *     priority     INT          NOT NULL,
 *     color        VARCHAR(15)  NOT NULL,
 *     description  TEXT,
 *     created_at   TIMESTAMP    NOT NULL,
 *     received_at  TIMESTAMP    NOT NULL,
 *     status       VARCHAR(20)  NOT NULL DEFAULT 'PENDING',
 *     completed_at TIMESTAMP    NULL          -- preenchido quando status = COMPLETED
 *   );
 *
 *   -- Migração para banco existente (rodar uma vez):
 *   ALTER TABLE occurrences
 *     ADD COLUMN completed_at TIMESTAMP NULL AFTER status;
 *
 *   CREATE TABLE attendance_nodes (
 *     id             VARCHAR(20)  PRIMARY KEY,
 *     name           VARCHAR(50)  NOT NULL,
 *     type           VARCHAR(20)  NOT NULL,
 *     status         VARCHAR(20)  NOT NULL DEFAULT 'AVAILABLE',
 *     last_heartbeat TIMESTAMP    NOT NULL
 *   );
 *
 *   CREATE TABLE metrics (
 *     id               BIGINT AUTO_INCREMENT PRIMARY KEY,
 *     occurrence_id    VARCHAR(36)  NOT NULL,
 *     node_id          VARCHAR(20)  NOT NULL,
 *     dispatched_at    TIMESTAMP    NOT NULL,
 *     ack_at           TIMESTAMP,
 *     response_time_ms BIGINT,
 *     retries          INT          NOT NULL DEFAULT 0
 *   );
 */
public class DatabaseManager {

    private static final String URL  = "jdbc:mysql://localhost:3306/emergency_db"
                                     + "?useSSL=false&serverTimezone=UTC";
    private static final String USER = "root";
    private static final String PASS = "maya7";

    private Connection conn;

    public DatabaseManager() {
        connect();
    }

    // ---------------------------------------------------------------
    // Conexão
    // ---------------------------------------------------------------
    private void connect() {
        try {
            conn = DriverManager.getConnection(URL, USER, PASS);
            System.out.println("[DB] Conexão estabelecida com MySQL");
        } catch (SQLException e) {
            System.err.println("[DB] ERRO ao conectar: " + e.getMessage());
            throw new RuntimeException("Falha crítica: banco indisponível", e);
        }
    }

    private Connection getConn() throws SQLException {
        if (conn == null || conn.isClosed()) {
            System.out.println("[DB] Reconectando...");
            connect();
        }
        return conn;
    }

    // ---------------------------------------------------------------
    // Ocorrências
    // ---------------------------------------------------------------

public void saveOccurrence(Occurrence occ) {
    String sql = """
        INSERT INTO occurrences
          (id, origin, type, priority, color, description, created_at, received_at, status)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'PENDING')
        """;

    try (PreparedStatement ps = getConn().prepareStatement(sql)) {

        // 🔍 DEBUG COMPLETO
        System.out.println("\n[DEBUG] Salvando ocorrência:");
        System.out.println("ID: " + occ.getId());
        System.out.println("Origin: " + occ.getOrigin());
        System.out.println("Type: " + occ.getType());
        System.out.println("Priority: " + occ.getPriority());
        System.out.println("Color: " + occ.getColor());
        System.out.println("CreatedAt: " + occ.getCreatedAt());
        System.out.println("ReceivedAt: " + occ.getReceivedAt());

        // 🚫 VALIDAÇÃO FORTE
        if (occ.getOrigin() == null ||
            occ.getType() == null ||
            occ.getColor() == null ||
            occ.getCreatedAt() == null ||
            occ.getReceivedAt() == null) {

            throw new RuntimeException("Occurrence inválida (NULL detectado): " + occ);
        }

        ps.setString(1, occ.getId());
        ps.setString(2, occ.getOrigin());
        ps.setString(3, occ.getType().name());
        ps.setInt   (4, occ.getPriority());
        ps.setString(5, occ.getColor().name());
        ps.setString(6, occ.getDescription());
        ps.setTimestamp(7, occ.getCreatedAt());
        ps.setTimestamp(8, occ.getReceivedAt());

        int rows = ps.executeUpdate();

        if (rows == 0) {
            throw new SQLException("INSERT não inseriu nenhuma linha.");
        }

        System.out.println("[DB] ✅ Ocorrência salva: " + occ.getId());

    } catch (SQLException e) {
        System.err.println("\nERRO REAL AO SALVAR OCCURRENCE:");
        e.printStackTrace(); 
        throw new RuntimeException(e);
    }
}
    public void saveOccurrence(String id, String origin, String type,
                               int priority, Color color,
                               String description,
                               Timestamp createdAt, Timestamp receivedAt) {

                                    if (origin == null ||
        type == null ||
        color == null ||
        createdAt == null ||
        receivedAt == null) {

        throw new RuntimeException("Occurrence inválida: campos obrigatórios null → " + origin);
    }

        String sql = """
            INSERT INTO occurrences
              (id, origin, type, priority, color, description, created_at, received_at, status)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'PENDING')
            """;
        try (PreparedStatement ps = getConn().prepareStatement(sql)) {
            ps.setString   (1, id);
            ps.setString   (2, origin);
            ps.setString   (3, type);
            ps.setInt      (4, priority);
            ps.setString   (5, color.name());
            ps.setString   (6, description);
            ps.setTimestamp(7, createdAt);
            ps.setTimestamp(8, receivedAt);
            ps.executeUpdate();
        } catch (SQLException e) {
            System.err.println("[DB] ERRO saveOccurrence: " + e.getMessage());
        }
    }

    /** Atualiza somente o status da ocorrência (ex: PENDING → DISPATCHED → ACKNOWLEDGED). */
    public void updateOccurrenceStatus(String id, String newStatus) {
        String sql = "UPDATE occurrences SET status = ? WHERE id = ?";
        try (PreparedStatement ps = getConn().prepareStatement(sql)) {
            ps.setString(1, newStatus);
            ps.setString(2, id);
            ps.executeUpdate();
        } catch (SQLException e) {
            System.err.println("[DB] ERRO updateOccurrenceStatus: " + e.getMessage());
        }
    }

    public void updateOccurrenceCompleted(String id, Timestamp completedAt) {
        String sql = "UPDATE occurrences SET status = 'COMPLETED', completed_at = ? WHERE id = ?";
        try (PreparedStatement ps = getConn().prepareStatement(sql)) {
            ps.setTimestamp(1, completedAt);
            ps.setString(2, id);
            ps.executeUpdate();
        } catch (SQLException e) {
            System.err.println("[DB] ERRO updateOccurrenceCompleted: " + e.getMessage());
        }
    }
    // ---------------------------------------------------------------
    // Nós de atendimento
    // ---------------------------------------------------------------
    public void registerNode(String nodeId, String name, String type) {
        String sql = """
            INSERT INTO attendance_nodes (id, name, type, status, last_heartbeat)
            VALUES (?, ?, ?, 'AVAILABLE', NOW())
            ON DUPLICATE KEY UPDATE last_heartbeat = NOW(), status = 'AVAILABLE'
            """;
        try (PreparedStatement ps = getConn().prepareStatement(sql)) {
            ps.setString(1, nodeId);
            ps.setString(2, name);
            ps.setString(3, type);
            ps.executeUpdate();
        } catch (SQLException e) {
            System.err.println("[DB] ERRO registerNode: " + e.getMessage());
        }
    }

    public void updateHeartbeat(String nodeId) {
        String sql = "UPDATE attendance_nodes SET last_heartbeat = NOW() WHERE id = ?";
        try (PreparedStatement ps = getConn().prepareStatement(sql)) {
            ps.setString(1, nodeId);
            ps.executeUpdate();
        } catch (SQLException e) {
            System.err.println("[DB] ERRO updateHeartbeat: " + e.getMessage());
        }
    }

    public void markNodeOffline(String nodeId) {
        String sql = "UPDATE attendance_nodes SET status = 'OFFLINE' WHERE id = ?";
        try (PreparedStatement ps = getConn().prepareStatement(sql)) {
            ps.setString(1, nodeId);
            ps.executeUpdate();
        } catch (SQLException e) {
            System.err.println("[DB] ERRO markNodeOffline: " + e.getMessage());
        }
    }

    // ---------------------------------------------------------------
    // Métricas
    // ---------------------------------------------------------------

    /**
     * Persiste uma entrada de métrica de despacho.
     *
     * @param occurrenceId  UUID da ocorrência
     * @param nodeId        ID do nó que recebeu o despacho
     * @param dispatchedAt  timestamp do envio pelo servidor central
     * @param ackAt         timestamp do ACK recebido pelo servidor central
     * @param responseTimeMs tempo entre dispatchedAt e ackAt (ms)
     * @param retries       número de tentativas do TCPClient (1 = sem retry)
     */
    public void saveMetric(String occurrenceId, String nodeId,
                            Timestamp dispatchedAt, Timestamp ackAt,
                            long responseTimeMs, int retries) {
        String sql = """
            INSERT INTO metrics
              (occurrence_id, node_id, dispatched_at, ack_at, response_time_ms, retries)
            VALUES (?, ?, ?, ?, ?, ?)
            """;
        try (PreparedStatement ps = getConn().prepareStatement(sql)) {
            ps.setString   (1, occurrenceId);
            ps.setString   (2, nodeId);
            ps.setTimestamp(3, dispatchedAt);
            ps.setTimestamp(4, ackAt);
            ps.setLong     (5, responseTimeMs);
            ps.setInt      (6, retries);
            ps.executeUpdate();
        } catch (SQLException e) {
            System.err.println("[DB] ERRO saveMetric: " + e.getMessage());
        }
    }


    public void saveTestResult(String testName, String phase,
                          String occurrenceId, int priority,
                          String type, boolean success,
                          long latencyMs) {

    String sql = """
        INSERT INTO test_results
        (test_name, phase, occurrence_id, priority, type, success, latency_ms)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """;

    try (PreparedStatement ps = getConn().prepareStatement(sql)) {
        ps.setString(1, testName);
        ps.setString(2, phase);
        ps.setString(3, occurrenceId);
        ps.setInt   (4, priority);
        ps.setString(5, type);
        ps.setBoolean(6, success);
        ps.setLong  (7, latencyMs);
        ps.executeUpdate();
    } catch (SQLException e) {
        e.printStackTrace();
    }
}

    // ---------------------------------------------------------------
    // Encerramento
    // ---------------------------------------------------------------
    public void close() {
        try {
            if (conn != null && !conn.isClosed()) {
                conn.close();
                System.out.println("[DB] Conexão encerrada");
            }
        } catch (SQLException e) {
            System.err.println("[DB] ERRO ao fechar conexão: " + e.getMessage());
        }
    }
}