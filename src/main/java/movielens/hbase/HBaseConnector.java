package movielens.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class HBaseConnector {

    // Noms des tables
    public static final String TABLE_MOVIE_STATS    = "movie_stats";
    public static final String TABLE_GENRE_STATS    = "genre_stats";
    public static final String TABLE_REAL_TIME      = "real_time_ratings";
    public static final String TABLE_ALERTS         = "rating_alerts";
    public static final String TABLE_RECOMMENDATIONS = "recommendations";

    // Column families
    public static final byte[] CF_INFO    = Bytes.toBytes("info");
    public static final byte[] CF_RATINGS = Bytes.toBytes("ratings");
    public static final byte[] CF_META    = Bytes.toBytes("meta");

    private final Connection connection;
    private final Admin admin;

    public HBaseConnector() throws IOException {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "localhost");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        this.connection = ConnectionFactory.createConnection(conf);
        this.admin      = connection.getAdmin();
    }

    public void createTablesIfNotExist() throws IOException {

        createTable(TABLE_MOVIE_STATS,
            CF_INFO, CF_RATINGS, CF_META);

        createTable(TABLE_GENRE_STATS,
            CF_INFO, CF_RATINGS);

        createTable(TABLE_REAL_TIME,
            CF_INFO, CF_RATINGS);

        createTable(TABLE_ALERTS,
            CF_INFO, CF_META);

        createTable(TABLE_RECOMMENDATIONS,
            CF_INFO, CF_META);

        System.out.println("✓ Tables HBase créées/vérifiées.");
    }

    private void createTable(String tableName, byte[]... columnFamilies)
            throws IOException {

        TableName tn = TableName.valueOf(tableName);
        if (admin.tableExists(tn)) {
            System.out.println("  Table déjà existante : " + tableName);
            return;
        }

        TableDescriptorBuilder builder =
            TableDescriptorBuilder.newBuilder(tn);

        for (byte[] cf : columnFamilies) {
            builder.setColumnFamily(
                ColumnFamilyDescriptorBuilder.newBuilder(cf)
                    .setMaxVersions(5)      // garder 5 versions
                    .setTimeToLive(86400 * 7) // TTL : 7 jours
                    .build()
            );
        }

        admin.createTable(builder.build());
        System.out.println("  ✓ Table créée : " + tableName);
    }

    public Table getTable(String tableName) throws IOException {
        return connection.getTable(TableName.valueOf(tableName));
    }

    public void close() throws IOException {
        admin.close();
        connection.close();
    }
}