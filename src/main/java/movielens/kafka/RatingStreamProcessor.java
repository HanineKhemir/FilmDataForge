package movielens.kafka;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.api.java.function.ForeachPartitionFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.*;
import org.apache.spark.sql.types.*;
import static org.apache.spark.sql.functions.*;

import java.io.IOException;

public class RatingStreamProcessor {

    private static final String BROKERS = "localhost:9092";
    private static final String TOPIC   = "movie-rating";

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

        // ── Lire depuis Kafka ──
        Dataset<org.apache.spark.sql.Row> raw = spark.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", BROKERS)
                .option("subscribe", TOPIC)
                .option("startingOffsets", "earliest")
                .option("failOnDataLoss", "false")
                .load()
                .selectExpr("CAST(value AS STRING) as raw");

        // ── Schéma JSON pour Letterboxd ──
        StructType jsonSchema = new StructType()
                .add("userId",    DataTypes.StringType)
                .add("movieId",   DataTypes.StringType)
                .add("rating",    DataTypes.DoubleType)
        .add("ratingRaw", DataTypes.DoubleType)
        .add("title",     DataTypes.StringType)
        .add("voteCount", DataTypes.IntegerType)
                .add("source",    DataTypes.StringType)
                .add("timestamp", DataTypes.LongType);

        // ── Détecter le format : JSON (Letterboxd) ou :: (MovieLens) ──
        // Format MovieLens  : "1::1193::5::978300760"
        // Format Letterboxd : {"userId":"john","movieId":"inception","rating":4.0,...}

        // Parser MovieLens (format ::)
        Dataset<org.apache.spark.sql.Row> movielensParsed = raw
                .filter(col("raw").isNotNull())
                .filter(not(col("raw").startsWith("{")))  // pas du JSON
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

        // Parser Letterboxd (format JSON)
        Dataset<org.apache.spark.sql.Row> letterboxdParsed = raw
                .filter(col("raw").isNotNull())
                .filter(col("raw").startsWith("{"))  // JSON
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
        Dataset<org.apache.spark.sql.Row> unified = movielensParsed.union(letterboxdParsed);

        // ── Agrégation : moyenne par film ──
        Dataset<org.apache.spark.sql.Row> aggregated = unified
                .groupBy("movieId", "source")
                .agg(
                    round(avg("rating"), 2).alias("avgRating"),
                    count("rating").alias("totalVotes")
                );

        // ── Stats globales par source ──
        Dataset<org.apache.spark.sql.Row> sourceStats = unified
                .groupBy("source")
                .agg(
                    count("rating").alias("totalRatings"),
                    round(avg("rating"), 2).alias("avgRating"),
                    approx_count_distinct("movieId").alias("uniqueMovies")
                );

        // ════════════════════════════
        // OUTPUT 1 — Console (résultats toutes les 5s)
        // ════════════════════════════
        StreamingQuery consoleQuery = aggregated
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
        StreamingQuery sourceQuery = sourceStats
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
        StreamingQuery hdfsQuery = unified
                .writeStream()
                .outputMode("append")
                .format("csv")
                .option("path", outputPath + "/raw-stream")
                .option("checkpointLocation", outputPath + "/checkpoint-raw")
                .option("header", "true")
                .trigger(Trigger.ProcessingTime("10 seconds"))
                .queryName("hdfs_archive")
                .start();

        // ════════════════════════════
        // OUTPUT 4 — HBase (résultats agrégés)
        // ════════════════════════════
        StreamingQuery hbaseQuery = aggregated
                .writeStream()
                .outputMode("update")
                .foreachBatch((batch, batchId) -> {
                    writeToHBase(batch);
                })
                .trigger(Trigger.ProcessingTime("10 seconds"))
                .queryName("hbase_writer")
                .start();

        System.out.println("\n✓ Stream processor démarré !");
        System.out.println("  - console_output  : agrégations toutes les 5s");
        System.out.println("  - source_stats    : stats par source toutes les 10s");
        System.out.println("  - hdfs_archive    : données brutes dans HDFS");
        System.out.println("  - hbase_writer    : résultats dans HBase");
        System.out.println("\nEn attente de messages sur : " + TOPIC);

        spark.streams().awaitAnyTermination();
    }

    private static void writeToHBase(Dataset<org.apache.spark.sql.Row> batch) {
        batch.foreachPartition((ForeachPartitionFunction<org.apache.spark.sql.Row>) rows -> {
            Configuration config = HBaseConfiguration.create();
            config.set("hbase.zookeeper.quorum", "localhost");
            config.set("hbase.zookeeper.property.clientPort", "2181");

            try (Connection connection = ConnectionFactory.createConnection(config);
                 Table table = connection.getTable(
                     TableName.valueOf("movie_stats"))) {

                while (rows.hasNext()) {
                    org.apache.spark.sql.Row row = rows.next();

                    String movieId  = String.valueOf(row.getAs("movieId"));
                    Object avgObj   = row.getAs("avgRating");
                    Object votesObj = row.getAs("totalVotes");
                    String source   = row.getAs("source");

                    if (movieId == null || movieId.equals("null")) continue;

                    // Clé : movieId_source (ex: "inception_letterboxd")
                    String rowKey = movieId + "_" + source;
                    Put put = new Put(Bytes.toBytes(rowKey));

                    if (avgObj != null) {
                        put.addColumn(
                            Bytes.toBytes("ratings"),
                            Bytes.toBytes("avgRating"),
                            Bytes.toBytes(avgObj.toString())
                        );
                    }
                    if (votesObj != null) {
                        put.addColumn(
                            Bytes.toBytes("ratings"),
                            Bytes.toBytes("totalVotes"),
                            Bytes.toBytes(votesObj.toString())
                        );
                    }
                    if (source != null) {
                        put.addColumn(
                            Bytes.toBytes("info"),
                            Bytes.toBytes("source"),
                            Bytes.toBytes(source)
                        );
                    }
                    put.addColumn(
                        Bytes.toBytes("info"),
                        Bytes.toBytes("lastUpdated"),
                        Bytes.toBytes(String.valueOf(System.currentTimeMillis()))
                    );

                    table.put(put);
                }
            } catch (IOException e) {
                throw new RuntimeException("HBase write error", e);
            }
        });
    }
}