package client;

import config.HBaseHelper;
import config.TutorialConfig;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.support.AbstractApplicationContext;

import java.io.IOException;

/**
 * Created by donghoon on 2016. 1. 17..
 */
public class PutTutorial {
    public static void main(String[] args) throws IOException {
        AbstractApplicationContext ctx = new AnnotationConfigApplicationContext(TutorialConfig.class);
        Configuration conf = ctx.getBean("hBaseConfiguration", Configuration.class);
        HBaseHelper helper = ctx.getBean("hBaseHelper", HBaseHelper.class);

        helper.dropTable("testtable");
        helper.createTable("testtable", "colfam1");
        // vv PutExample
        Connection connection = ConnectionFactory.createConnection(conf);
        Table table = connection.getTable(TableName.valueOf("testtable")); // co PutExample-2-NewTable Instantiate a new client.

        Put put = new Put(Bytes.toBytes("row1")); // co PutExample-3-NewPut Create put with specific row.

        put.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"),
                Bytes.toBytes("val1")); // co PutExample-4-AddCol1 Add a column, whose name is "colfam1:qual1", to the put.
        put.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual2"),
                Bytes.toBytes("val2")); // co PutExample-4-AddCol2 Add another column, whose name is "colfam1:qual2", to the put.

        table.put(put); // co PutExample-5-DoPut Store row with column into the HBase table.
        table.close(); // co PutExample-6-DoPut Close table and connection instances to free resources.
        connection.close();
        // ^^ PutExample
        helper.close();
        // vv PutExample
    }
}
