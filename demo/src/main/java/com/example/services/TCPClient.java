package com.example.services;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;

import com.example.model.Occurrence;
import com.example.model.Occurrence.Type;

public class TCPClient {
    private static final String SERVER_HOST    = "localhost";
    private static final int    SERVER_PORT    = 9000;
    private static final int    CONNECT_TIMEOUT = 5_000;  // ms
    private static final int    READ_TIMEOUT    = 10_000; // ms
    private static final int    MAX_RETRIES     = 3;
    private static final long   RETRY_DELAY_MS  = 2_000;
    private final String origin; // "POSTO_A" | "POSTO_B" | "APP_192"

    public TCPClient(String origin) {
        this.origin = origin;
    }


    public String send(Occurrence occurrence) throws Exception {
        Exception lastException = null;
 
        for (int attempt = 1; attempt <= MAX_RETRIES; attempt++) {
            try {
                return trySend(occurrence);
            } catch (Exception e) {
                lastException = e;
                System.out.println("Tentativa " + attempt + "/" + MAX_RETRIES + " falhou: " + e.getMessage());
                if (attempt < MAX_RETRIES) {
                    Thread.sleep(RETRY_DELAY_MS);
                }
            }
        }
 
        throw new Exception("Falha após " + MAX_RETRIES + " tentativas", lastException);
    }

    // ---------------------------------------------------------------
    // Tentativa única de envio
    // ---------------------------------------------------------------
    private String trySend(Occurrence occurrence) throws Exception {
        try (Socket socket = new Socket()) {
 
            socket.connect(new InetSocketAddress(SERVER_HOST, SERVER_PORT), CONNECT_TIMEOUT);
            socket.setSoTimeout(READ_TIMEOUT);
 
            // Ordem importa: ObjectOutputStream antes de ObjectInputStream
            ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
            out.flush();
            ObjectInputStream  in  = new ObjectInputStream(socket.getInputStream());
 
            System.out.println("Enviando ocorrência: " + occurrence);
            out.writeObject(occurrence);
            out.flush();
 
            // Aguarda confirmação do servidor
            Object response = in.readObject();
 
            if (response instanceof String resp) {
                if (resp.startsWith("ACK:")) {
                    System.out.println("Confirmado pelo servidor → " + resp);
                    return resp.substring(4); // retorna o ID
                } else {
                    throw new Exception("Servidor retornou erro: " + resp);
                }
            }
 
            throw new Exception("Resposta inesperada do servidor: " + response);
        }
    }

    public String getOrigin() { return origin; }

    public static void main(String[] args) throws Exception {
        TCPClient client = new TCPClient("TESTE");
        Occurrence oc = new Occurrence("Posto 1",Type.CARDIAC_ARREST, 1,Occurrence.Color.RED,
                "Paciente com dor torácica intensa, 67 anos");
        String id = client.send(oc);
        System.out.println("Ocorrência registrada com ID: " + id);
    }


}
