package com.example;

import java.sql.Connection;
import java.sql.DriverManager;

public class Main {
    public static void main(String[] args) {
        String url  = "jdbc:mysql://localhost:3306/emergency_db?useSSL=false&serverTimezone=UTC";
        String user = "root";
        String pass = "maya7";

        try (Connection conn = DriverManager.getConnection(url, user, pass)) {
            System.out.println("Conectado ao MySQL com sucesso!");
        } catch (Exception e) {
            System.out.println("Falha na conexão: " + e.getMessage());
        }
    }
}