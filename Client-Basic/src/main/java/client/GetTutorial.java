package client;

import config.HBaseHelper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Created by donghoon on 2016. 1. 17..
 */
public class GetTutorial {
    private Logger logger = LoggerFactory.getLogger(GetTutorial.class);
    private Configuration conf;
    private HBaseHelper helper;

    public GetTutorial(Configuration conf, HBaseHelper helper) {
        this.conf = conf;
        this.helper = helper;
    }

    public String get() throws IOException {
        try (Connection connection = ConnectionFactory.createConnection(conf);
             Table table = connection.getTable(TableName.valueOf("testtable"))) {

            if (!helper.existsTable("testtable")) {
                helper.createTable("testtable", "colfam1");
            }

            Get get = new Get(Bytes.toBytes("row1")); // co GetExample-3-NewGet Create get with specific row.

            get.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1")); // co GetExample-4-AddCol Add a column to the get.

            Result result = table.get(get); // co GetExample-5-DoGet Retrieve row with selected columns from HBase.

            byte[] val = result.getValue(Bytes.toBytes("colfam1"),
                    Bytes.toBytes("qual1")); // co GetExample-6-GetValue Get a specific value for the given column.

            logger.info("Value: " + Bytes.toString(val)); // co GetExample-7-Print Print out the value while converting it back.

            return Bytes.toString(val);
        } finally {
            helper.close();
        }
    }
}
