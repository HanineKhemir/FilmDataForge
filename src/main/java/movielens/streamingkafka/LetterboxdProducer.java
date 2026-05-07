package movielens.streamingkafka;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.serialization.StringSerializer;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.io.IOException;
import java.util.*;

public class LetterboxdProducer {

    private static final String TOPIC   = "letterboxd-ratings";
    private static final String BROKERS = "localhost:9092";

    // Films à scraper en rotation
    private static final List<String> FILMS = Arrays.asList(
        "inception",
        "the-dark-knight",
        "parasite-2019",
        "everything-everywhere-all-at-once",
        "the-godfather",
        "oppenheimer",
        "poor-things-2023",
        "past-lives"
    );

    private static final String USER_AGENT =
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) " +
        "AppleWebKit/537.36 Chrome/124.0.0.0 Safari/537.36";

    public static void main(String[] args) throws Exception {

        boolean loop = args.length > 0 && args[0].equals("--loop");
        int maxPages = args.length > 1 ? Integer.parseInt(args[1]) : 3;

        Map<String, String> sessionCookies = initCookies();

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                  StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                  StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 3);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 100);

        ensureTopicExists(props, TOPIC, 1, (short) 1);

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        System.out.println("Letterboxd Producer démarré");
        System.out.println("Broker : " + BROKERS);
        System.out.println("Topic  : " + TOPIC);
        System.out.println("Mode   : " + (loop ? "boucle infinie" : "un seul cycle"));

        try {
            do {
                List<String> films = new ArrayList<>(FILMS);
                Collections.shuffle(films);

                for (String film : films) {
                    scrapeAndSend(film, producer, maxPages, sessionCookies);
                    // Pause entre films
                    Thread.sleep(3000 + (long)(Math.random() * 2000));
                }

                if (loop) {
                    System.out.println("\nCycle terminé. Prochain dans 60s...");
                    Thread.sleep(60_000);
                }

            } while (loop);

        } catch (InterruptedException e) {
            System.out.println("Arrêt du producer.");
        } finally {
            producer.flush();
            producer.close();
        }
    }

    private static void scrapeAndSend(
            String filmSlug,
            KafkaProducer<String, String> producer,
            int maxPages,
            Map<String, String> sessionCookies) {

        System.out.println("\nScraping : " + filmSlug);
        int sent = 0;

        for (int page = 1; page <= maxPages; page++) {
            try {
                // URL de la page members/fans du film
                String url = "https://letterboxd.com/film/" +
                    filmSlug + "/members/rated/0.5-5/page/" + page + "/";

                Document doc = null;
                int maxAttempts = 2;
                for (int attempt = 1; attempt <= maxAttempts; attempt++) {
                    org.jsoup.Connection.Response response = Jsoup.connect(url)
                            .userAgent(USER_AGENT)
                            .referrer("https://letterboxd.com/")
                            .header("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8")
                            .header("Accept-Language", "en-US,en;q=0.9")
                            .header("Cache-Control", "no-cache")
                            .header("Pragma", "no-cache")
                            .cookies(sessionCookies)
                            .timeout(15_000)
                            .ignoreHttpErrors(true)
                            .followRedirects(true)
                            .execute();

                    int status = response.statusCode();
                    if (status == 200) {
                        doc = response.parse();
                        break;
                    }

                    if (status == 403 && attempt < maxAttempts) {
                        System.err.println("  403 détecté, rafraîchissement des cookies...");
                        sessionCookies.clear();
                        sessionCookies.putAll(initCookies());
                        Thread.sleep(1200L * attempt);
                        continue;
                    }

                    throw new IOException("HTTP error fetching URL. Status=" + status + ", URL=[" + url + "]");
                }

                if (doc == null) {
                    System.err.println("  Impossible de récupérer la page " + page + " pour " + filmSlug);
                    break;
                }

                // Chaque ligne du tableau = un rating utilisateur
                Elements rows = doc.select("tr.table-film-detail");

                if (rows.isEmpty()) {
                    // Essayer un autre sélecteur
                    rows = doc.select("tr");
                }

                if (rows.isEmpty()) break; // plus de pages

                for (Element row : rows) {
                    // Extraire username
                    Element userEl = row.selectFirst("a[href*=/]");
                    if (userEl == null) continue;

                    String username = userEl.attr("href")
                        .replaceAll("^/", "").replaceAll("/$", "");
                    if (username.isEmpty() || username.contains("/")) continue;

                    // Extraire la note (classe CSS "rated-X" où X = 1-10)
                    Element ratingEl = row.selectFirst("[class*=rated-]");
                    if (ratingEl == null) continue;

                    String cssClass = ratingEl.attr("class");
                    int ratingRaw = extractRatingFromClass(cssClass);
                    if (ratingRaw <= 0) continue;

                    // Convertir de l'échelle Letterboxd (1-10) à (0.5-5.0)
                    double rating = ratingRaw / 2.0;

                    // Construire le message JSON manuellement
                    long timestamp = System.currentTimeMillis();
                    String message = String.format(
                        "{\"userId\":\"%s\",\"movieId\":\"%s\"," +
                        "\"rating\":%.1f,\"ratingRaw\":%d," +
                        "\"source\":\"letterboxd\"," +
                        "\"timestamp\":%d}",
                        username, filmSlug,
                        rating, ratingRaw, timestamp
                    );

                    ProducerRecord<String, String> record =
                        new ProducerRecord<>(TOPIC, filmSlug, message);

                    producer.send(record, (meta, ex) -> {
                        if (ex != null)
                            System.err.println("Erreur Kafka : " + ex.getMessage());
                    });

                    sent++;
                    Thread.sleep(50); // 50ms entre messages
                }

                System.out.println("  Page " + page + " : " + sent + " ratings envoyés");

                // Pause entre pages pour respecter Letterboxd
                Thread.sleep(500 + (long)(Math.random() * 500));

            } catch (IOException e) {
                System.err.println("  Erreur HTTP page " + page +
                    " pour " + filmSlug + " : " + e.getMessage());
                break;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }

        System.out.println("  Total envoyé pour " + filmSlug + " : " + sent);
        producer.flush();
    }

    private static int extractRatingFromClass(String cssClass) {
        // La classe CSS ressemble à "rating rated-8" → ratingRaw = 8
        for (String cls : cssClass.split("\\s+")) {
            if (cls.startsWith("rated-")) {
                try {
                    return Integer.parseInt(cls.replace("rated-", ""));
                } catch (NumberFormatException e) {
                    // ignore
                }
            }
        }
        return -1;
    }

    private static Map<String, String> initCookies() {
        try {
            org.jsoup.Connection.Response response = Jsoup.connect("https://letterboxd.com/")
                    .userAgent(USER_AGENT)
                    .referrer("https://letterboxd.com/")
                    .header("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8")
                    .header("Accept-Language", "en-US,en;q=0.9")
                    .timeout(10_000)
                    .ignoreHttpErrors(true)
                    .followRedirects(true)
                    .execute();
            return new HashMap<>(response.cookies());
        } catch (IOException e) {
            System.err.println("Impossible d'initialiser les cookies Letterboxd : " + e.getMessage());
            return new HashMap<>();
        }
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
