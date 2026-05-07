package movielens.mock;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.serialization.StringSerializer;
import java.io.*;
import java.util.*;

public class RatingProducer {

    private static final String TOPIC   = "movielens-ratings";
    private static final String BROKERS = "localhost:9092";

    public static void main(String[] args) throws Exception {

        if (args.length != 1) {
            System.err.println("Usage: RatingProducer <ratings.dat path>");
            System.exit(1);
        }

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 3);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 100);

        // Ensure topic exists before producing
        ensureTopicExists(props, TOPIC, 1, (short) 1);

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        System.out.println("Démarrage du producer Kafka...");
        System.out.println("Broker : " + BROKERS);
        System.out.println("Topic  : " + TOPIC);

        int count = 0;

        try (BufferedReader br = new BufferedReader(
                new FileReader(args[0]))) {

            String line;
            while ((line = br.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty()) continue;
                String[] parts = line.split("::");
                if (parts.length != 4) continue;

                String movieId = parts[1];
                ProducerRecord<String, String> record =
                    new ProducerRecord<>(TOPIC, movieId, line);

                producer.send(record, (metadata, exception) -> {
                    if (exception != null)
                        System.err.println("Erreur : " + exception.getMessage());
                });

                count++;
                if (count % 1000 == 0)
                    System.out.println(count + " ratings envoyés...");

                Thread.sleep(10);
            }
        }

        producer.flush();
        producer.close();
        System.out.println("Terminé ! " + count + " ratings envoyés.");
    }

    private static void ensureTopicExists(
            Properties props,
            String topic,
            int partitions,
            short replicationFactor) {
        try (AdminClient admin = AdminClient.create(props)) {
            NewTopic newTopic = new NewTopic(topic, partitions, replicationFactor);
            admin.createTopics(Collections.singleton(newTopic)).all().get();
            System.out.println("Topic créé : " + topic);
        } catch (java.util.concurrent.ExecutionException e) {
            if (e.getCause() instanceof TopicExistsException) {
                System.out.println("Topic déjà existant : " + topic);
            } else {
                System.err.println("Impossible de créer le topic " + topic + " : " + e.getMessage());
            }
        } catch (Exception e) {
            System.err.println("AdminClient error pour le topic " + topic + " : " + e.getMessage());
        }
    }
}
