package com.example.db;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;

import com.example.model.Occurrence;

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

    /**
     * Persiste uma ocorrência nova.
     * Colunas conforme o script: id, origin, type, priority, color,
     * description, received_at. Status fica como DEFAULT 'PENDING'.
     */
    public void saveOccurrence(Occurrence occ) {
        String sql = """
            INSERT INTO occurrences
              (id, origin, type, priority, color, description, received_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """;
        try (PreparedStatement ps = getConn().prepareStatement(sql)) {
            ps.setString   (1, occ.getId());
            ps.setString   (2, occ.getOrigin());
            ps.setString   (3, occ.getType().name());
            ps.setInt      (4, occ.getPriority());
            ps.setString   (5, occ.getColor().name());
            ps.setString   (6, occ.getDescription());
            ps.setTimestamp(7, occ.getReceivedAt());
            ps.executeUpdate();
        } catch (SQLException e) {
            System.err.println("[DB] ERRO saveOccurrence: " + e.getMessage());
        }
    }

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
     * Salva métrica de despacho.
     * completed_at fica NULL por enquanto — pode ser atualizado
     * quando o nó finalizar o atendimento futuramente.
     */
    public void saveMetric(String occurrenceId, String nodeId,
                            Timestamp dispatchedAt, Timestamp ackAt,
                            long responseTimeMs, int retries) {
        String sql = """
            INSERT INTO metrics
              (occurrence_id, node_id, dispatched_at, ack_at, completed_at, response_time_ms, retries)
            VALUES (?, ?, ?, ?, NULL, ?, ?)
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