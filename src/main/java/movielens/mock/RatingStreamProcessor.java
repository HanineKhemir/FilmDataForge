package movielens.mock;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
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

        Dataset<Row> raw = spark.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", BROKERS)
                .option("subscribe", TOPIC)
                .option("startingOffsets", "earliest")
                .option("failOnDataLoss", "false")
                .load()
                .selectExpr("CAST(value AS STRING) as raw");

        StructType jsonSchema = new StructType()
                .add("userId",    DataTypes.StringType)
                .add("movieId",   DataTypes.StringType)
                .add("rating",    DataTypes.DoubleType)
                .add("ratingRaw", DataTypes.DoubleType)
                .add("title",     DataTypes.StringType)
                .add("voteCount", DataTypes.IntegerType)
                .add("source",    DataTypes.StringType)
                .add("timestamp", DataTypes.LongType);

        Dataset<Row> movielensParsed = raw
                .filter(col("raw").isNotNull())
                .filter(not(col("raw").startsWith("{")))
                .select(
                    split(col("raw"), "::").getItem(0).cast("string").alias("userId"),
                    split(col("raw"), "::").getItem(1).cast("string").alias("movieId"),
                    split(col("raw"), "::").getItem(2).cast("double").alias("rating"),
                    lit("movielens").alias("source")
                )
                .filter(col("movieId").isNotNull())
                .filter(col("rating").isNotNull());

        Dataset<Row> letterboxdParsed = raw
                .filter(col("raw").isNotNull())
                .filter(col("raw").startsWith("{"))
                .select(from_json(col("raw"), jsonSchema).alias("data"))
                .select(
                    col("data.userId").alias("userId"),
                    col("data.movieId").alias("movieId"),
                    col("data.rating").alias("rating"),
                    col("data.source").alias("source")
                )
                .filter(col("movieId").isNotNull())
                .filter(col("rating").isNotNull());

        Dataset<Row> unified = movielensParsed.union(letterboxdParsed);

        Dataset<Row> aggregated = unified
                .groupBy("movieId", "source")
                .agg(
                    round(avg("rating"), 2).alias("avgRating"),
                    count("rating").alias("totalVotes")
                );

        Dataset<Row> sourceStats = unified
                .groupBy("source")
                .agg(
                    count("rating").alias("totalRatings"),
                    round(avg("rating"), 2).alias("avgRating"),
                    approxCountDistinct("movieId").alias("uniqueMovies")
                );

        aggregated
                .writeStream()
                .outputMode("complete")
                .format("console")
                .option("truncate", false)
                .option("numRows", 20)
                .trigger(Trigger.ProcessingTime("5 seconds"))
                .queryName("console_output")
                .start();

        sourceStats
                .writeStream()
                .outputMode("complete")
                .format("console")
                .option("truncate", false)
                .trigger(Trigger.ProcessingTime("10 seconds"))
                .queryName("source_stats")
                .start();

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

        // ForeachWriter replaces foreachBatch (not available in Spark 2.2)
        aggregated
                .writeStream()
                .outputMode("update")
                .foreach(new ForeachWriter<Row>() {
                    private transient Connection connection;
                    private transient Table table;

                    @Override
                    public boolean open(long partitionId, long version) {
                        try {
                            Configuration config = HBaseConfiguration.create();
                            config.set("hbase.zookeeper.quorum", "localhost");
                            config.set("hbase.zookeeper.property.clientPort", "2181");
                            connection = ConnectionFactory.createConnection(config);
                            table = connection.getTable(TableName.valueOf("movie_stats"));
                            return true;
                        } catch (IOException e) {
                            System.err.println("HBase open error: " + e.getMessage());
                            return false;
                        }
                    }

                    @Override
                    public void process(Row row) {
                        try {
                            String movieId  = String.valueOf(row.getAs("movieId"));
                            Object avgObj   = row.getAs("avgRating");
                            Object votesObj = row.getAs("totalVotes");
                            String source   = row.getAs("source");

                            if (movieId == null || movieId.equals("null")) return;

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
                        } catch (IOException e) {
                            throw new RuntimeException("HBase write error", e);
                        }
                    }

                    @Override
                    public void close(Throwable errorOrNull) {
                        try {
                            if (table != null) table.close();
                            if (connection != null) connection.close();
                        } catch (IOException e) {
                            // ignore on close
                        }
                    }
                })
                .trigger(Trigger.ProcessingTime("10 seconds"))
                .queryName("hbase_writer")
                .start();

        System.out.println("\n Stream processor started!");
        System.out.println("  - console_output : aggregations every 5s");
        System.out.println("  - source_stats   : source stats every 10s");
        System.out.println("  - hdfs_archive   : raw data to HDFS");
        System.out.println("  - hbase_writer   : results to HBase");
        System.out.println("\nWaiting for messages on: " + TOPIC);

        spark.streams().awaitAnyTermination();
    }
}
