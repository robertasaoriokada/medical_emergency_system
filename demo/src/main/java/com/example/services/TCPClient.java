package com.example.services;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;

import com.example.model.Occurrence;
import com.example.model.Occurrence.Type;

public class TCPClient {
    private static final String SERVER_HOST     = "localhost";
    private static final int    SERVER_PORT     = 9000;
    private static final int    CONNECT_TIMEOUT = 5_000;
    private static final int    READ_TIMEOUT    = 10_000;
    private static final int    MAX_RETRIES     = 3;
    private static final long   RETRY_DELAY_MS  = 2_000;

    private final String origin;

    public TCPClient(String origin) {
        this.origin = origin;
    }

    // ---------------------------------------------------------------
    // Resultado de envio — carrega o ID confirmado e quantas
    // tentativas foram necessárias (1 = sem retry, 2+ = houve retry)
    // ---------------------------------------------------------------
    public record SendResult(String occurrenceId, int attempts) {}

    /**
     * Envia uma ocorrência ao servidor central com até MAX_RETRIES tentativas.
     * Retorna SendResult com o ID confirmado e o número de tentativas usadas.
     */
    public SendResult send(Occurrence occurrence) throws Exception {
        Exception lastException = null;

        for (int attempt = 1; attempt <= MAX_RETRIES; attempt++) {
            try {
                String id = trySend(occurrence);
                return new SendResult(id, attempt); // attempt == número real de tentativas
            } catch (Exception e) {
                lastException = e;
                System.out.printf("Tentativa %d/%d falhou: %s%n",
                        attempt, MAX_RETRIES, e.getMessage());
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

            ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
            out.flush();
            ObjectInputStream  in  = new ObjectInputStream(socket.getInputStream());

            System.out.println("Enviando ocorrência: " + occurrence);
            out.writeObject(occurrence);
            out.flush();

            Object response = in.readObject();

            if (response instanceof String resp) {
                if (resp.startsWith("ACK:")) {
                    System.out.println("Confirmado pelo servidor → " + resp);
                    return resp.substring(4);
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

        Occurrence oc = new Occurrence("Posto 1", Type.CARDIAC_ARREST, 1, Occurrence.Color.RED, "Paciente com dor torácica intensa, 67 anos");
        Occurrence oc2 = new Occurrence("Posto 2", Type.OBSTETRIC, 4, Occurrence.Color.BLUE, "Paciente grávida com pressão levemente alta, 31 anos");
        Occurrence oc3 = new Occurrence("Posto 3", Type.TRAUMA, 5, Occurrence.Color.GREEN, "Torceu o pé, 13 anos");
        Occurrence oc4 = new Occurrence("Posto 1", Type.TRAUMA, 1, Occurrence.Color.RED, "Impalado");

        SendResult result = client.send(oc);
        SendResult result2 = client.send(oc2);
        SendResult result3 = client.send(oc3);
        SendResult result4 = client.send(oc4);

        System.out.printf("Ocorrência registrada | ID: %s | tentativas: %d%n",
                result.occurrenceId(), result.attempts());
        System.out.printf("Ocorrência registrada | ID: %s | tentativas: %d%n",
                result2.occurrenceId(), result2.attempts());
        System.out.printf("Ocorrência registrada | ID: %s | tentativas: %d%n",
                result3.occurrenceId(), result3.attempts());
        System.out.printf("Ocorrência registrada | ID: %s | tentativas: %d%n",
                result4.occurrenceId(), result4.attempts());



    }
}