package client;

import config.HBaseHelper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    public void put(String tableName, String colfam, String row,
                    String[] qualifier, String[] val) throws IOException {
        try (Connection connection = ConnectionFactory.createConnection(conf);
             Table table = connection.getTable(TableName.valueOf(tableName))) {
            helper.dropTable(tableName);
            helper.createTable(tableName, colfam);

            Put put = new Put(Bytes.toBytes(row)); // co PutExample-3-NewPut Create put with specific row.

            put.addColumn(Bytes.toBytes(colfam), Bytes.toBytes(qualifier[0]),
                    Bytes.toBytes(val[0])); // co PutExample-4-AddCol1 Add a column, whose name is "colfam1:qual1", to the put.
            put.addColumn(Bytes.toBytes(colfam), Bytes.toBytes(qualifier[1]),
                    Bytes.toBytes(val[1])); // co PutExample-4-AddCol2 Add another column, whose name is "colfam1:qual2", to the put.

            table.put(put); // co PutExample-5-DoPut Store row with column into the HBase table.
        }
    }
}
