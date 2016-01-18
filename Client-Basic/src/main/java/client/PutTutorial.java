package client;

import config.HBaseHelper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * Created by donghoon on 2016. 1. 17..
 */
public class PutTutorial {
    private Configuration conf;
    private HBaseHelper helper;

    public PutTutorial(Configuration conf, HBaseHelper helper) {
        this.conf = conf;
        this.helper = helper;
    }

    public void put(String tableName, String row, String colfam,
                    String[] qualifier, String[] val) throws IOException {
        try (Connection connection = ConnectionFactory.createConnection(conf);
             Table table = connection.getTable(TableName.valueOf(tableName))) {
            helper.dropTable(tableName);
            helper.createTable(tableName, colfam);

            Put put = new Put(Bytes.toBytes(row));

            put.addColumn(Bytes.toBytes(colfam), Bytes.toBytes(qualifier[0]),
                    Bytes.toBytes(val[0]));
            put.addColumn(Bytes.toBytes(colfam), Bytes.toBytes(qualifier[1]),
                    Bytes.toBytes(val[1]));

            table.put(put);
        }
    }
}
