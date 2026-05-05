package movielens.hbase;

import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class HBaseWriter {

    private final HBaseConnector connector;

    public HBaseWriter() throws IOException {
        this.connector = new HBaseConnector();
        this.connector.createTablesIfNotExist();
    }

    public void writeMovieStats(
            String movieId, double avgRating,
            long totalVotes, double stddev,
            String source) throws IOException {

        try (Table table = connector.getTable(
                HBaseConnector.TABLE_MOVIE_STATS)) {

            Put put = new Put(Bytes.toBytes(movieId));

            put.addColumn(HBaseConnector.CF_INFO,
                Bytes.toBytes("source"),
                Bytes.toBytes(source));

            put.addColumn(HBaseConnector.CF_RATINGS,
                Bytes.toBytes("avgRating"),
                Bytes.toBytes(String.valueOf(avgRating)));

            put.addColumn(HBaseConnector.CF_RATINGS,
                Bytes.toBytes("totalVotes"),
                Bytes.toBytes(String.valueOf(totalVotes)));

            put.addColumn(HBaseConnector.CF_RATINGS,
                Bytes.toBytes("stddev"),
                Bytes.toBytes(String.valueOf(stddev)));

            put.addColumn(HBaseConnector.CF_RATINGS,
                Bytes.toBytes("lastUpdated"),
                Bytes.toBytes(String.valueOf(System.currentTimeMillis())));

            table.put(put);
        }
    }

    public void writeRealTimeRating(
            String userId, String movieId,
            double rating, String source,
            long timestamp) throws IOException {

        try (Table table = connector.getTable(
                HBaseConnector.TABLE_REAL_TIME)) {

            String rowKey = movieId + "_" +
                String.format("%013d", timestamp);

            Put put = new Put(Bytes.toBytes(rowKey));

            put.addColumn(HBaseConnector.CF_INFO,
                Bytes.toBytes("userId"),
                Bytes.toBytes(userId));

            put.addColumn(HBaseConnector.CF_INFO,
                Bytes.toBytes("movieId"),
                Bytes.toBytes(movieId));

            put.addColumn(HBaseConnector.CF_RATINGS,
                Bytes.toBytes("rating"),
                Bytes.toBytes(String.valueOf(rating)));

            put.addColumn(HBaseConnector.CF_META,
                Bytes.toBytes("source"),
                Bytes.toBytes(source));

            put.addColumn(HBaseConnector.CF_META,
                Bytes.toBytes("timestamp"),
                Bytes.toBytes(String.valueOf(timestamp)));

            table.put(put);
        }
    }

    public void writeAlert(
            String movieId, String alertType,
            int total, int extremeCount,
            double avgRating) throws IOException {

        try (Table table = connector.getTable(
                HBaseConnector.TABLE_ALERTS)) {

            String rowKey = movieId + "_" +
                System.currentTimeMillis();

            Put put = new Put(Bytes.toBytes(rowKey));

            put.addColumn(HBaseConnector.CF_INFO,
                Bytes.toBytes("movieId"),
                Bytes.toBytes(movieId));

            put.addColumn(HBaseConnector.CF_INFO,
                Bytes.toBytes("alertType"),
                Bytes.toBytes(alertType));

            put.addColumn(HBaseConnector.CF_META,
                Bytes.toBytes("total"),
                Bytes.toBytes(String.valueOf(total)));

            put.addColumn(HBaseConnector.CF_META,
                Bytes.toBytes("extremeCount"),
                Bytes.toBytes(String.valueOf(extremeCount)));

            put.addColumn(HBaseConnector.CF_META,
                Bytes.toBytes("avgRating"),
                Bytes.toBytes(String.valueOf(avgRating)));

            table.put(put);
        }
    }

    public void printMovieStats(String movieId) throws IOException {
        try (Table table = connector.getTable(
                HBaseConnector.TABLE_MOVIE_STATS)) {

            Get get = new Get(Bytes.toBytes(movieId));
            Result result = table.get(get);

            if (result.isEmpty()) {
                System.out.println("Aucune stat pour : " + movieId);
                return;
            }

            String avg = Bytes.toString(result.getValue(
                HBaseConnector.CF_RATINGS, Bytes.toBytes("avgRating")));
            String votes = Bytes.toString(result.getValue(
                HBaseConnector.CF_RATINGS, Bytes.toBytes("totalVotes")));

            System.out.printf("Film: %-30s | avg: %s | votes: %s%n",
                movieId, avg, votes);
        }
    }

    public void close() throws IOException {
        connector.close();
    }
}