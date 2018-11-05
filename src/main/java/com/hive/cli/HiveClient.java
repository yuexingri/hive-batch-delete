package com.hive.cli;

import com.google.common.collect.Lists;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Formatter;
import java.util.List;

public class HiveClient {
    private static final Logger logger = LoggerFactory.getLogger(HiveClient.class);

    private HiveConf hiveConf = null;
    private HiveMetaStoreClient metaStoreClient = null;
    private Connection connection = null;
    private Statement statement = null;
    private ResultSet resultSet = null;

    HiveClient() {
        hiveConf = new HiveConf(HiveClient.class);
    }

    private HiveMetaStoreClient getMetaStoreClient() throws Exception {
        if (metaStoreClient == null) {
            metaStoreClient = new HiveMetaStoreClient(hiveConf);
        }
        return metaStoreClient;
    }

    private List<String> getAllTablesByDatabase(String database) throws Exception {
        HiveMetaStoreClient metaStoreClient = getMetaStoreClient();
        return metaStoreClient.getAllTables(database);
    }

    private List<String> getAllDatabases() throws Exception {
        HiveMetaStoreClient metaStoreClient = getMetaStoreClient();
        return metaStoreClient.getAllDatabases();
    }

    public void dropAllTables() throws Exception {
        List<String> allDatabases = getAllDatabases() == null ? Lists.<String>newArrayList() : getAllDatabases();
        for (String database : allDatabases) {
            List<String> tables = getAllTablesByDatabase(database) == null ? Lists.<String>newArrayList() : getAllTablesByDatabase(database);
            for (String table : tables) {
                dropTable(database, table);
            }
        }
    }

    public void dropTable(String database, String table) throws Exception {
        try {
            Connection connection = getConnection(database);
            statement = connection.createStatement();
            Formatter formatter = new Formatter();
            formatter.format("DROP TABLE IF EXISTS %s", table);
            String sql = formatter.toString();
            statement.execute(sql);
        } catch (Exception e) {
            logger.info(e.getMessage());
        } finally {
            closeQuietly(connection);
            closeQuietly(statement);
            closeQuietly(resultSet);
        }
    }

    private Connection getConnection(String database) {
        try {
            Class.forName("org.apache.hive.jdbc.HiveDriver");
            String host = hiveConf.get("hadoop.proxyuser.hive.hosts");
            String port = hiveConf.get("hive.server2.thrift.port");
            String user = hiveConf.get("javax.jdo.option.ConnectionUserName");
            String passwd = hiveConf.get("hive_metastore_user_passwd");
            String url = "jdbc:hive2://" + host + ":" + port + "/" + database;
            return DriverManager.getConnection(url, user, passwd);
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
//            closeQuietly(connection);
        }
    }

    public static void closeQuietly(final AutoCloseable closeable) {
        try {
            if (closeable != null) {
                closeable.close();
            }
        } catch (final Exception ioe) {
            logger.debug("", ioe);
        }
    }
}
