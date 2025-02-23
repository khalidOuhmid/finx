package com.example.backend.data;

import com.datastax.oss.driver.api.core.CqlSession;

import java.net.InetSocketAddress;

public class CassandraConnectionTest {

    public static void main(String[] args) {
        try (CqlSession session = CqlSession.builder()
                .addContactPoint(new InetSocketAddress("127.0.0.1", 9042))
                .withLocalDatacenter("datacenter1")
                .build()) {

            System.out.println("Connexion réussie à Cassandra !");
            System.out.println("Keyspaces disponibles : " + session.getMetadata().getKeyspaces().keySet());

        } catch (Exception e) {
            System.err.println("Erreur lors de la connexion à Cassandra : " + e.getMessage());
            e.printStackTrace();
        }
    }
}
