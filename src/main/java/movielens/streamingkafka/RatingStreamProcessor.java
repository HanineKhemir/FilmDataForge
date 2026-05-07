package movielens.streamingkafka;

import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.*;
import org.apache.spark.sql.types.*;

import static org.apache.spark.sql.functions.*;

public class RatingStreamProcessor {

    private static final String BROKERS = "localhost:9092";
    private static final String TOPIC_MOVIELENS = "movielens-ratings";
    private static final String TOPIC_LETTERBOXD = "letterboxd-ratings";

    public static void main(String[] args) throws Exception {

        if (args.length != 1) {
            System.err.println("Usage: RatingStreamProcessor <output_path>");
            System.exit(1);
        }

        String outputPath = args[0];

        SparkSession spark = SparkSession.builder()
                .appName("MovieLens Stream Processor")
                .getOrCreate();

        spark.sparkContext().setLogLevel("WARN");

        // ═══════════════════════════════════════════════════════════
        // Read from MovieLens topic (format: "userId::movieId::rating::timestamp")
        // ═══════════════════════════════════════════════════════════
        Dataset<Row> movieLensRaw = spark.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", BROKERS)
                .option("subscribe", TOPIC_MOVIELENS)
                .option("startingOffsets", "earliest")
                .option("failOnDataLoss", "false")
                .load()
                .selectExpr("CAST(value AS STRING) as raw");

        Dataset<Row> movielensParsed = movieLensRaw
                .select(
                    split(col("raw"), "::").getItem(0)
                        .cast("string").alias("userId"),
                    split(col("raw"), "::").getItem(1)
                        .cast("string").alias("movieId"),
                    split(col("raw"), "::").getItem(2)
                        .cast("double").alias("rating"),
                    lit("movielens").alias("source")
                )
                .filter(col("movieId").isNotNull())
                .filter(col("rating").isNotNull());

        // ═══════════════════════════════════════════════════════════
        // Read from Letterboxd topic (format: JSON)
        // ═══════════════════════════════════════════════════════════
        StructType jsonSchema = new StructType()
                .add("userId",    DataTypes.StringType)
                .add("movieId",   DataTypes.StringType)
                .add("rating",    DataTypes.DoubleType)
                .add("source",    DataTypes.StringType)
                .add("timestamp", DataTypes.LongType);

        Dataset<Row> letterboxdRaw = spark.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", BROKERS)
                .option("subscribe", TOPIC_LETTERBOXD)
                .option("startingOffsets", "earliest")
                .option("failOnDataLoss", "false")
                .load()
                .selectExpr("CAST(value AS STRING) as raw");

        Dataset<Row> letterboxdParsed = letterboxdRaw
                .select(
                    from_json(col("raw"), jsonSchema).alias("data")
                )
                .select(
                    col("data.userId").alias("userId"),
                    col("data.movieId").alias("movieId"),
                    col("data.rating").alias("rating"),
                    col("data.source").alias("source")
                )
                .filter(col("movieId").isNotNull())
                .filter(col("rating").isNotNull());

        // ── Unifier les deux sources ──
        Dataset<Row> unified = movielensParsed.union(letterboxdParsed);

        // ── Agrégation : moyenne par film ──
        Dataset<Row> aggregated = unified
                .groupBy("movieId", "source")
                .agg(
                    round(avg("rating"), 2).alias("avgRating"),
                    count("rating").alias("totalVotes")
                );

        // ── Stats globales par source ──
        Dataset<Row> sourceStats = unified
                .groupBy("source")
                .agg(
                    count("rating").alias("totalRatings"),
                    round(avg("rating"), 2).alias("avgRating"),
                    approxCountDistinct("movieId").alias("uniqueMovies")
                );

        // ════════════════════════════
        // OUTPUT 1 — Console (résultats toutes les 5s)
        // ════════════════════════════
        aggregated
            .writeStream()
            .outputMode("complete")
            .format("console")
            .option("truncate", false)
            .option("numRows", 20)
            .trigger(Trigger.ProcessingTime("5 seconds"))
            .queryName("console_output")
            .start();

        // ════════════════════════════
        // OUTPUT 2 — Stats par source (console)
        // ════════════════════════════
        sourceStats
            .writeStream()
            .outputMode("complete")
            .format("console")
            .option("truncate", false)
            .trigger(Trigger.ProcessingTime("10 seconds"))
            .queryName("source_stats")
            .start();

        // ════════════════════════════
        // OUTPUT 3 — HDFS archive (CSV)
        // ════════════════════════════
        unified
            .writeStream()
            .outputMode("append")
            .format("csv")
            .option("path", outputPath + "/raw-stream")
            .option("checkpointLocation", outputPath + "/checkpoint-raw")
            .option("header", "true")
            .trigger(Trigger.ProcessingTime("10 seconds"))
            .queryName("hdfs_archive")
            .start();

        System.out.println("\n✓ Stream processor démarré !");
        System.out.println("  - Topics: " + TOPIC_MOVIELENS + ", " + TOPIC_LETTERBOXD);
        System.out.println("  - console_output  : agrégations toutes les 5s");
        System.out.println("  - source_stats    : stats par source toutes les 10s");
        System.out.println("  - hdfs_archive    : données brutes dans HDFS");

        spark.streams().awaitAnyTermination();
    }

}
