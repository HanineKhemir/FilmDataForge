package movielens.kafka;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

public class LetterboxdProducer {

    private static final String TOPIC = "movie-rating";
    private static final String BROKERS = "localhost:9092";
    private static final String TMDB_BASE_URL = "https://api.themoviedb.org/3/movie/popular";

    public static void main(String[] args) throws Exception {
        boolean loop = args.length > 0 && args[0].equals("--loop");
        int maxPages = args.length > 1 ? Integer.parseInt(args[1]) : 1;

        String token = getTmdbToken();
        if (token == null || token.trim().isEmpty()) {
            System.err.println("TMDB token manquant. Définis TMDB_TOKEN dans l'environnement.");
            System.exit(1);
        }

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 3);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 100);

        ensureTopicExists(props, TOPIC, 1, (short) 1);

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        System.out.println("TMDB Producer démarré");
        System.out.println("Broker : " + BROKERS);
        System.out.println("Topic  : " + TOPIC);
        System.out.println("Mode   : " + (loop ? "boucle infinie" : "un seul cycle"));

        try {
            do {
                int sent = 0;
                for (int page = 1; page <= maxPages; page++) {
                    List<TmdbMovie> movies = fetchPopularMovies(token, page);
                    sent += sendMovies(movies, producer);
                    Thread.sleep(500);
                }

                System.out.println("\nCycle terminé — " + sent + " messages envoyés");
                if (loop) {
                    System.out.println("Prochain cycle dans 60s...");
                    Thread.sleep(60_000);
                }
            } while (loop);
        } catch (InterruptedException e) {
            System.out.println("Arrêt du producer.");
            Thread.currentThread().interrupt();
        } finally {
            producer.flush();
            producer.close();
        }
    }

    private static List<TmdbMovie> fetchPopularMovies(String token, int page) throws IOException {
        String url = TMDB_BASE_URL + "?language=en-US&page=" + page;
        HttpURLConnection connection = (HttpURLConnection) new URL(url).openConnection();
        connection.setRequestMethod("GET");
        connection.setRequestProperty("accept", "application/json");
        connection.setRequestProperty("Authorization", "Bearer " + token);
        connection.setConnectTimeout(10_000);
        connection.setReadTimeout(15_000);

        int status = connection.getResponseCode();
        if (status != 200) {
            throw new IOException("TMDB error HTTP " + status + " for URL " + url);
        }

        try (InputStream input = connection.getInputStream()) {
            ObjectMapper mapper = new ObjectMapper();
            JsonNode root = mapper.readTree(input);
            JsonNode results = root.path("results");

            List<TmdbMovie> movies = new ArrayList<>();
            for (JsonNode node : results) {
                TmdbMovie movie = new TmdbMovie(
                        node.path("id").asText(),
                        node.path("title").asText(),
                        node.path("vote_average").asDouble(),
                        node.path("vote_count").asInt()
                );
                movies.add(movie);
            }
            return movies;
        } finally {
            connection.disconnect();
        }
    }

    private static int sendMovies(List<TmdbMovie> movies, KafkaProducer<String, String> producer)
            throws InterruptedException {
        int sent = 0;
        List<TmdbMovie> shuffled = new ArrayList<>(movies);
        Collections.shuffle(shuffled);

        for (TmdbMovie movie : shuffled) {
            double rating = Math.round((movie.voteAverage / 2.0) * 10.0) / 10.0;
            long timestamp = System.currentTimeMillis();
            String message = String.format(
                    "{\"userId\":\"tmdb\",\"movieId\":\"%s\"," +
                            "\"title\":\"%s\",\"rating\":%.1f," +
                            "\"ratingRaw\":%.1f,\"voteCount\":%d," +
                            "\"source\":\"tmdb\",\"timestamp\":%d}",
                    movie.id, escapeJson(movie.title), rating, movie.voteAverage,
                    movie.voteCount, timestamp
            );

            ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, movie.id, message);
            producer.send(record, (meta, ex) -> {
                if (ex != null) {
                    System.err.println("Erreur Kafka : " + ex.getMessage());
                }
            });
            sent++;
            Thread.sleep(80);
        }

        producer.flush();
        return sent;
    }

    private static String escapeJson(String value) {
        return value == null ? "" : value.replace("\\", "\\\\").replace("\"", "\\\"");
    }

    private static String getTmdbToken() {
        String token = System.getenv("TMDB_TOKEN");
        if (token == null || token.trim().isEmpty()) {
            token = System.getenv("TMDB_BEARER_TOKEN");
        }
        return token;
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

    private static class TmdbMovie {
        private final String id;
        private final String title;
        private final double voteAverage;
        private final int voteCount;

        private TmdbMovie(String id, String title, double voteAverage, int voteCount) {
            this.id = id;
            this.title = title;
            this.voteAverage = voteAverage;
            this.voteCount = voteCount;
        }
    }
}